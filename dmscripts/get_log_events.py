import boto3
import json
import os
import uuid


class LogRetriever(object):
    """
    """
    def __init__(self, arn, group):
        """
        """
        self.group = group
        # create session
        sts_client = boto3.client('sts')
        session_name = str(uuid.uuid4())
        sts_response = sts_client.assume_role(
            RoleArn=arn,
            RoleSessionName=session_name
        )
        session = boto3.session.Session(
            aws_access_key_id=sts_response['Credentials']['AccessKeyId'],
            aws_secret_access_key=sts_response['Credentials']['SecretAccessKey'],
            aws_session_token=sts_response['Credentials']['SessionToken']
        )
        self.client = session.client('logs')

    def describe_log_streams(self, earliest_epoch, latest_epoch):
        """
        """
        streams = []
        # fetch first log stream
        response = self.client.describe_log_streams(
            logGroupName=self.group,
            orderBy='LastEventTime',
            descending=True
        )
        # check epoch range and add stream logStreamName
        for stream in response['logStreams']:
            if earliest_epoch < stream['lastEventTimestamp'] and stream['firstEventTimestamp'] < latest_epoch:
                streams.append(stream['logStreamName'])
        while len(response['logStreams']) > 0 and 'nextToken' in response.keys():
            response = self.client.describe_log_streams(
                logGroupName=self.group,
                orderBy='LastEventTime',
                descending=True,
                nextToken=response['nextToken']
            )
            # check epoch range and add stream logStreamName
            for stream in response['logStreams']:
                if earliest_epoch < stream['lastEventTimestamp'] and stream['firstEventTimestamp'] < latest_epoch:
                    streams.append(stream['logStreamName'])
        return streams

    def get_log_events(self, stream, directory):
        """
        """
        counter = 0
        directory = os.path.join(directory, self.group, stream)
        os.makedirs(directory, exist_ok=True)
        # fetch the logs first page
        response = self.client.get_log_events(
            logGroupName=self.group,
            logStreamName=f'{stream}',
            startFromHead=True
        )
        with open(f'{directory}/{counter}.json', 'w') as f:
            json.dump(response, f)
        # iterate to fetch the rest of the pages
        while len(response['events']) > 0:
            counter += 1
            response = self.client.get_log_events(
                logGroupName=self.group,
                logStreamName=stream,
                startFromHead=True,
                nextToken=response['nextForwardToken']
            )
            with open(f'{directory}/{counter}.json', 'w') as f:
                json.dump(response, f)
        return counter + 1

    def get_log_event_in_epoch_range(self, directory, earliest_epoch, latest_epoch):
        """
        """
        # get all streams in range
        streams = self.describe_log_streams(earliest_epoch, latest_epoch)
        for stream in streams:
            # write log events in directory
            self.get_log_events(directory, stream)
