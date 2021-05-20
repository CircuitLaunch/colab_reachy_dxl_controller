#!/usr/bin/env python3

import bisect
import math
import numpy as np
from copy import deepcopy
from collections import OrderedDict
from typing import List

import rospy
import actionlib
import motion_control.bezier as bezier
from rospy import ROSException
from std_msgs.msg import String
from sensor_msgs.msg import JointState
from trajectory_msgs.msg import JointTrajectoryPoint
from control_msgs.msg import (
    FollowJointTrajectoryAction,
    FollowJointTrajectoryGoal,
    FollowJointTrajectoryFeedback,
    FollowJointTrajectoryResult)
from std_srvs.srv import SetBool, SetBoolResponse

from colab_reachy_dxl_controller.srv import ReportJointStatus, ReportJointStatusResponse
from colab_reachy_dxl_controller.srv import ReportJointTemperatures, ReportJointTemperaturesResponse

from DXL import *

class ReachyActionServer:

    ############################################################################
    # CONSTRUCTOR
    ############################################################################
    def __init__(self, name):
        # Initialize the ROS node
        rospy.init_node(name, log_level = rospy.DEBUG)

        self.name = name

        self.armContinuousTrajectories = rospy.get_param('~continuous_trajectories')
        self.goalDelayTolerance = rospy.get_param('~goal_delay_tolerance')
        self.controlRate = rospy.Rate(rospy.get_param('~control_rate'))
        self.reportRate = rospy.Rate(rospy.get_param("~report_rate"))

        # Establish a connection to the dxl controller
        self.u2d2 = DXLPort(rospy.get_param('~io'), rospy.get_param('~baud'))

        # Obtain references to all the actuators
        self.reachyDXLs = OrderedDict()
        self.reachyDXLs['r_shoulder_pitch'] = self.u2d2.getDXL(10)
        self.reachyDXLs['r_shoulder_roll'] = self.u2d2.getDXL(11)
        self.reachyDXLs['r_arm_yaw'] = self.u2d2.getDXL(12)
        self.reachyDXLs['r_elbow_pitch'] = self.u2d2.getDXL(13)
        self.reachyDXLs['r_forearm_yaw'] = self.u2d2.getDXL(14)
        self.reachyDXLs['r_wrist_pitch'] = self.u2d2.getDXL(15)
        self.reachyDXLs['r_wrist_roll'] = self.u2d2.getDXL(16)
        self.reachyDXLs['r_gripper'] = self.u2d2.getDXL(17)

        self.reachyDXLs['l_shoulder_pitch'] = self.u2d2.getDXL(20)
        self.reachyDXLs['l_shoulder_roll'] = self.u2d2.getDXL(21)
        self.reachyDXLs['l_arm_yaw'] = self.u2d2.getDXL(22)
        self.reachyDXLs['l_elbow_pitch'] = self.u2d2.getDXL(23)
        self.reachyDXLs['l_forearm_yaw'] = self.u2d2.getDXL(24)
        self.reachyDXLs['l_wrist_pitch'] = self.u2d2.getDXL(25)
        self.reachyDXLs['l_wrist_roll'] = self.u2d2.getDXL(26)
        self.reachyDXLs['l_gripper'] = self.u2d2.getDXL(27)

        self.rightArm = [
            'r_shoulder_pitch',
            'r_shoulder_roll',
            'r_arm_yaw',
            'r_elbow_pitch',
            'r_forearm_yaw',
            'r_wrist_pitch',
            'r_wrist_roll'
        ]

        self.leftArm = [
            'l_shoulder_pitch',
            'l_shoulder_roll',
            'l_arm_yaw',
            'l_elbow_pitch',
            'l_forearm_yaw',
            'l_wrist_pitch',
            'l_wrist_roll'
        ]

        # Blast disable torque instruction to all the servos
        self.u2d2.syncWrite(RAM_TORQUE_ENABLE, 1, dict(zip(self.reachyDXLs.keys(), [0] * len(self.reachyDXLs.keys()))))

        # Should we?
        '''
        result = self.u2d2.syncWrite(EEPROM_RETURN_DELAY_TIME, 1, dict(zip(self.reachyDXLs.keys(), [0] * len(self.reachyDXLs.keys())))
        '''

        # Create a joint state publisher
        self.rightArmJointStatePublisher = rospy.Publisher('right_arm_controller/joint_states', JointState, queue_size = 10)
        self.leftArmJointStatePublisher = rospy.Publisher('left_arm_controller/joint_states', JointState, queue_size = 10)

        self.recoverJointSubscriber = rospy.Subscriber('recover_joint', String, self.recoverJoint, queue_size = 10)

        self.reportJointStatus = rospy.Service('report_joint_status', ReportJointStatus, self.reportJointStatus)
        self.reportJointTemperatures = rospy.Service('report_joint_temperatures', ReportJointTemperatures, self.reportJointTemperatures)

        # Create a set compliance services
        self.rightArmComplianceService = rospy.Service('right_arm_set_compliant', SetBool, self.setRightArmCompliance)
        self.leftArmComplianceService = rospy.Service('left_arm_set_compliant', SetBool, self.setLeftArmCompliance)
        self.rightGripperComplianceService = rospy.Service('right_gripper_set_compliant', SetBool, self.setRightGripperCompliance)
        self.leftGripperComplianceService = rospy.Service('left_gripper_set_compliant', SetBool, self.setLeftGripperCompliance)

        # Create and start an action servers
        self.rightArmActionServer = actionlib.SimpleActionServer(
            'right_arm_controller/follow_joint_trajectory',
            FollowJointTrajectoryAction,
            execute_cb = self.actionServerCallback,
            auto_start = False
        )
        self.leftArmActionServer = actionlib.SimpleActionServer(
            'left_arm_controller/follow_joint_trajectory',
            FollowJointTrajectoryAction,
            execute_cb = self.actionServerCallback,
            auto_start = False
        )

    ############################################################################
    def launch(self):
        self.rightArmActionServer.start()
        self.leftArmActionServer.start()
        reportSpinner = self.reportRate
        rospy.on_shutdown(self.onShutdown)
        while not rospy.is_shutdown():
            self.reportJointStates()
            reportSpinner.sleep()

    ############################################################################
    def onShutdown(self):
        # Blast disable torque instruction to all the servos
        self.u2d2.syncWrite(RAM_TORQUE_ENABLE, 1, dict(zip(self.reachyDXLs.keys(), [0] * len(self.reachyDXLs.keys()))))
        rospy.sleep(1.0)

    ############################################################################
    def recoverJoint(self, msg):
        name = msg.data
        if name in self.reachyDXLs:
            dxl = self.reachyDXLs[name]
            dxl.recover()

    ############################################################################
    def setRightArmCompliance(self, value: SetBool):
        self.setArmCompliance('right', value)

    ############################################################################
    def setLeftArmCompliance(self, value: SetBool):
        self.setArmCompliance('left', value)

    ############################################################################
    def setArmCompliance(self, side: str, value: SetBool):
        if side == 'right':
            jointNames = self.rightArm
        elif side == 'left':
            jointNames = self.leftArm
        elif side == 'both':
            jointNames = self.rightArm + self.leftArm
        else:
            return SetBoolResponse(success=False, message=f'{side} is not a valid side specification')

        ids = [self.reachyDXLs[name].id for name in list(set(jointNames) & set(self.reachyDXLs.keys()))]
        bit = 0 if value.data else 1
        self.u2d2.syncWrite(RAM_TORQUE_ENABLE, 1, dict(zip(ids, [bit] * len(ids))))

        return SetBoolResponse(success=True, message=f'{side} arm compliance set to {"True" if value.data else "False"}')

    ############################################################################
    def setRightGripperCompliance(self, value: SetBool):
        self.setGripperCompliance('right', value)

    ############################################################################
    def setLeftGripperCompliance(self, value: SetBool):
        self.setGripperCompliance('left', value)

    ############################################################################
    def setGripperCompliance(self, side: str, value: SetBool):
        if side == 'right':
            jointNames = ['right_gripper']
        elif side == 'left':
            jointNames = ['left_gripper']
        elif side == 'both':
            jointNames = ['right_gripper', 'left_gripper']
        else:
            return SetBoolResponse(success=False, message=f'{side} is not a valid side specification')

        ids = [self.reachyDXLs[name].id for name in list(set(jointNames) & set(self.reachyDXLs.keys()))]
        bit = 0 if value.data else 1
        result = self.u2d2.syncWrite(RAM_TORQUE_ENABLE, 1, dict(zip(ids, [bit] * len(ids))))

        return SetBoolResponse(success=True, message=f'{side} gripper compliance set to {"True" if value.data else "False"}')

    ############################################################################
    def getCurrentPositions(self, jointNames: List[str]):
        # ids = [self.reachyDXLs[name].id for name in jointNames]
        # syncRead not available in protocol 1.0
        # result, data = self.u2d2.syncRead(RAM_PRESENT_POSITION, 2, ids)
        # return [math.radians(data[self.reachyDXLs[name].id]) for name in jointNames]
        return [math.radians(self.reachyDXLs[name].presentPosition) for name in list(set(jointNames) & set(self.reachyDXLs.keys()))]

    ############################################################################
    def getTrajectoryComponents(self, trajectories: List[JointTrajectoryPoint]):
        positionFlag = True
        velocityFlag = len(trajectories[0].velocities) != 0 and len(trajectories[-1].velocities) != 0
        accelerationFlag = len(trajectories[0].accelerations) != 0 and len(trajectories[-1].accelerations) != 0
        return { 'positions': positionFlag, 'velocities': velocityFlag, 'accelerations': accelerationFlag }

    ############################################################################
    def getBezierCoefficients(self, jointNames: List[str], trajectoryPoints: List[JointTrajectoryPoint], components: dict):
        jointCount = len(jointNames)
        trajectoryPointCount = len(trajectoryPoints)
        dimensions = len(components)
        bMatrix = np.zeros(shape=(jointCount, trajectoryPointCount, dimensions-1, 4))
        for jointIndex in range(jointCount):
            trajectoryPoint = np.zeros(shape=(trajectoryPointCount, dimensions))
            for id, point in enumerate(trajectoryPoints):
                currentPVA = list()
                currentPVA.append(point.positions[jointIndex])
                if dimensions['velocities']:
                    currentPVA.append(point.velocities[jointIndex])
                if dimensions['accelrations']:
                    currentPVA.append(point.accelerations[jointIndex])
                trajectoryPoint[jointIndex, :] = currentPVA
            controlPoints = bezier.de_boor_control_pts(trajectoryPoint)
            bMatrix[jnt, :, :, :] = bezier.bezier_coefficients(trajectoryPoint, controlPoints)
        return bMatrix

    ############################################################################
    def getBezierPoint(self, bMatrix, idx, t, cmdTime, components):
        trajectoryPoint = JointTrajectoryPoint()
        trajectoryPoint.time_from_start  = rospy.Duration(cmdTime)
        jointCount = bMatrix.shape[0]
        trajectoryPoint.positions = [0.0] * jointCount
        if dimensions['velocities']:
            trajectoryPoint.velocities = [0.0] * jointCount
        if dimensions['accelerations']:
            trajectoryPoint.accelerations = [0.0] * jointCount
        for jointIndex in range(jointCount):
            bPoint = bezier.bezier_point(bMatrix[jointIndex, :, :, :], idx, t)
            trajectoryPoint.positions[jointIndex] = bPoint[0]
            if components['velocity']:
                trajectoryPoint.velocities[jointIndex] = bPoint[1]
            if components['velocity']:
                trajectoryPoint.accelerations[jointIndex] = bPoint[2]
        return trajectoryPoint

    ############################################################################
    def commandJoints(self, jointNames: List[str], point: JointTrajectoryPoint):
        rospy.logdebug(f'{self.name}: Setting dxls to {point.positions}')
        if len(jointNames) != len(point.positions):
            rospy.logerr(f'{self.name}: Joint name and trajectory point mismatch')
            return false
        '''
        for i, name in enumerate(jointNames):
            if name not in self.reachyDXLs.keys():
                rospy.logerr(f'{self.name}: Joint "{name}" not found')
                return false
        '''
        # TODO: check for compliance
        ids = [self.reachyDXLs[name].id for name in list(set(jointNames) & set(self.reachyDXLs.keys()))]
        positions = [math.degrees(float(point.positions[i])) for i in range(len(point.positions))]
        self.u2d2.syncWrite(RAM_GOAL_POSITION, 2, dict(zip(ids, positions)))

    ############################################################################
    def updateFeedback(self, cmdPoint: JointTrajectoryPoint, jointNames: List[str], currentTime: float):
        validJointNames = list(set(jointNames) & set(self.reachyDXLs.keys()))
        actualPositions = self.getCurrentPositions(validJointNames)
        currentROSTime = rospy.Duration.from_sec(currentTime)

        feedback = FollowJointTrajectoryFeedback()
        feedback.header.stamp = rospy.Duration.from_sec(rospy.get_time())
        feedback.joint_names = validJointNames
        feedback.desired = cmdPoint
        feedback.desired.time_from_start = currentROSTime
        feedback.actual.positions = actualPositions
        feedback.actual.time_from_start = currentROSTime
        feedback.error.positions = list(map(operator.sub, cmdPoint, actualPositions))
        self.actionServer.publish_feedback(feedback)
        rospy.logdebug(f'{self.name}: Current positions [{actualPositions}]')

    '''
    ############################################################################
    def reportJointState(self):
        # TODO: need to check for invalid names and None DXL references
        jointState = JointState()
        jointState.header.stamp = rospy.Time.now()
        jointState.position = self.getCurrentPositions(self.rightArm)
        try:
            self.rightArmJointStatePublisher.publish(jointState)
        except ROSException:
            pass
        jointState.header.stamp = rospy.Time.now()
        jointState.position = self.getCurrentPosition(self.leftArm)
        try:
            self.leftArmJointStatePublisher.publish(jointState)
        except ROSException:
            pass
    '''

    ############################################################################
    def reportJointStatus(self, request: ReportJointStatus):
        jointStatus = ReportJointStatusResponse()
        jointStatus.header.stamp = rospy.Time.now()
        jointStatus.name = []
        jointStatus.model = []
        jointStatus.status = []
        jointStatus.error = []
        validNames = list(set(request.name) & set(self.reachyDXLs.keys()))
        for name in validNames:
            dxl = self.reachyDXLs[name]
            model, result, error = dxl.ping()
            modelStr = DXLPort.modelName(model)
            resultStr = self.u2d2.resultString(result)
            errorStr = self.u2d2.errorString(error)
            jointStatus.name.append(name)
            jointStatus.model.append(modelStr)
            jointStatus.status.append(resultStr)
            jointStatus.status.append(errorStr)
        return jointStatus

    ############################################################################
    def reportJointTemperatures(self, request: ReportJointTemperatures):
        # TODO: need to check for invalid names and None DXL references
        jointTemperatures = JointTemperaturesResponse()
        jointTemperatures.header.stamp = rospy.Time.now()
        jointTemperatures.name = request.name
        # syncRead not available in protocol 1.0
        # result, data = self.u2d2.syncRead(RAM_PRESENT_TEMPERATURE, 1, [self.reachyDXLs[name].id for name in request.name])
        #jointTemperatures.temperature = [data[self.reachyDXLs[name].id] for name in request.name]
        data = [self.reachyDXLs[name].presentTemperature for name in list(set(request.name) & set(self.reachyDXLs.keys()))]
        jointTemperatures.temperature = data
        return jointTemperatures

    ############################################################################
    def actionServerCallback(self, goal: FollowJointTrajectoryGoal):
        actionResult = FollowJointTrajectoryResult()
        jointNames = goal.trajectory.joint_names
        # Check for invalid joint names
        for jnt in jointNames:
            if jnt not in self.reachyDXLs.keys():
                rospy.logerr(f'{self.name}: Trajectory aborted (invalid joint name: {jnt})')
                actionResult.error_code = FollowJointTrajectoryResult.INVALID_JOINTS
                self.actionServer.set_aborted(result)
                return

        trajectoryPoints = goal.trajectory.points
        # Check for 0 trajectories
        trajectoryPointCount = len(trajectoryPoints)
        if trajectoryPointCount == 0:
            rospy.logerr(f'{self.name}: No trajectories')
            self.actionServer.set_aborted()
            return

        # Trace
        rospy.logwarn(
            f'{self.name}: Executing trajectory { trajectoryPoints[-1].time_from_start.to_sec() } s'
        )

        components = self.getTrajectoryComponents(trajectoryPoints)

        if trajectoryPointCount == 1:
            trajectoryPoint0 = JointTrajectoryPoint()
            trajectoryPoint0.positions = self.getCurrentPositions(jointNames)
            if components['velocities']:
                trajectoryPoint0.velocities = deepcopy(trajectoryPoints[0].velocities)
            if components['accelerations']:
                trajectoryPoint0.accelerations = deepcopy(trajectoryPoints[0].velocities)
            trajectoryPoint0.time_from_start = rospy.Duration(0)
            trajectoryPoints.insert(0, trajectoryPoint0)
            trajectoryPointCount = len(trajectoryPoints)

        if self.armContinuousTrajectories:
            if components['velocities']:
                trajectoryPoints[-1].velocities = [0.0] * len(jointNames)
            if components['accelerations']:
                trajectoryPoints[-1].accelerations = [0.0] * len(jointNames)

        pointTimes = [point.time_from_start.to_sec() for point in trajectoryPoints]

        try:
            bMatrix = self.getBezierCoefficients(jointNames, trajectoryPoints, components)
        except Exception as x:
            rospy.logerr(f'Failed Bezier computations for trajetory: {repr(x)}')
            self.actionServer.set_aborted()
            return

        now = rospy.get_time()

        startTime = goal.trajectory.header.stamp.to_sec()
        if startTime == 0.0: startTime = now

        duration = trajectoryPoints[-1].time_from_start.to_sec()

        elapsed = now - startTime

        if goal.goal_time_tolerance:
            goalTimeTolerance = goal.goal_time_tolerance.to_sec()
        else:
            goalTimeTolerance = self.goalDelayTolerance

        while not rospy.is_shutdown() and elapsed < duration + goalTimeTolerance:
            if self.actionServer.is_preempt_requested():
                rospy.loginfo(f'self.name: Trajectory preempted')
                self.actionServer.set_preempted()
                return

            elapsed = rospy.get_time() - startTime

            if elapsed >= 0:
                if elapsed < duration:
                    index = bisect.bisect(pointTimes, elapsed)
                    cmdTime = 0.0
                    t = 0.0
                    if index >= pointCount:
                        cmdTime = elapsed - pointTimes[-1]
                        t = 1.0
                    elif index >= 0:
                        cmdTime = elapsed - pointTimes[index - 1]
                        t = cmdTime / max(pointTimes[index] - pointTimes[index - 1], 0.001)
                    point = self.getBezierPoint(bMatrix, index, t, cmdTime, components)
                else:
                    point = trajectoryPoints[-1]

                if not self.commandJoints(jointNames, point):
                    self.actionServer.set_aborted()
                    rospy.logwarn(f'{self.name}: Trajectory aborted')
                    return

                self.updateFeedback(deepcopy(point), jointNames, elapsed)

            self.controlRate.sleep()

        rospy.loginfo(f'{self.name}: Trajectory complete')
        actionResult.error_code = FollowJointTrajectoryResult.SUCCESSFUL
        selt.actionServer.set_succeeded(actionResult)

def main():
    server = ReachyActionServer("reachy_dxl_controller")
    server.launch()

if __name__ == '__main__':
    main()
