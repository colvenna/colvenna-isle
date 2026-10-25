"""Isolated trusted ARM builder; dependency code receives no build credentials."""


REGION = "${AWS::Region}"


from colvenna_isle.region import regional

def template(baseline, source, policy, project, *, account):
    bucket, repository, prefix = baseline["DataBucket"], policy["repository"], policy["runtime_prefix"]
    cluster_arn = f"arn:aws:ecs:{REGION}:{account}:cluster/{baseline['Cluster']}"
    tags = [{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "builder"},
            {"Key": "SourceCommit", "Value": policy["source_commit"]}]
    repository_arn = f"arn:aws:ecr:{REGION}:{account}:repository/{repository}"
    role_prefix = f"arn:aws:iam::{account}:role/{prefix}*"
    runtime_files = [
        f"arn:aws:s3:::{bucket}/spaces/*/{kind}/gen_*/*"
        for kind in ["inputs", "pipeline-code", "task-contexts", "staging"]]
    statements = [
        # CodeBuild probes the exact source key with an unqualified HeadObject.
        # Body acquisition still uses the pinned SourceVersion below.
        {"Effect": "Allow", "Action": ["s3:GetObject"],
         "Resource": f"arn:aws:s3:::{bucket}/{source['key']}"},
        {"Effect": "Allow", "Action": ["s3:GetObjectVersion"],
         "Resource": f"arn:aws:s3:::{bucket}/{source['key']}",
         "Condition": {"StringEquals": {"s3:VersionId": source["version_id"]}}},
        {"Effect": "Allow", "Action": ["s3:GetBucketAcl", "s3:GetBucketLocation"],
         "Resource": f"arn:aws:s3:::{bucket}"},
        {"Effect": "Allow", "Action": ["s3:GetObjectVersion", "s3:PutObject"],
         "Resource": f"arn:aws:s3:::{bucket}/spaces/*/environment-builds/gen_*/*"},
        {"Effect": "Allow", "Action": ["ecr:GetAuthorizationToken"], "Resource": "*"},
        {"Effect": "Allow", "Action": [
            "ecr:BatchCheckLayerAvailability", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart",
            "ecr:CompleteLayerUpload", "ecr:PutImage", "ecr:DescribeImages",
            "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"], "Resource": repository_arn},
        {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
         "Resource": [
             {"Fn::Sub": f"arn:aws:logs:{REGION}:{account}:log-group:${{BuildLogs}}:log-stream:*"}]},
        {"Effect": "Allow", "Action": ["iam:CreateRole"], "Resource": role_prefix,
         "Condition": {"StringEquals": {"iam:PermissionsBoundary": {"Ref": "RuntimeBoundary"}}}},
        {"Effect": "Allow", "Action": [
            "iam:GetRole", "iam:TagRole", "iam:ListRolePolicies", "iam:GetRolePolicy",
            "iam:ListAttachedRolePolicies", "iam:PutRolePolicy"], "Resource": role_prefix},
        {"Effect": "Allow", "Action": ["iam:PassRole"], "Resource": role_prefix,
         "Condition": {"StringEquals": {"iam:PassedToService": [
             "lambda.amazonaws.com", "ecs-tasks.amazonaws.com"]}}},
        {"Effect": "Allow", "Action": ["iam:PassRole"],
         "Resource": {"Fn::GetAtt": ["ExecutionRole", "Arn"]},
         "Condition": {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}},
        {"Effect": "Allow", "Action": ["lambda:CreateFunction", "lambda:TagResource",
                                     "lambda:GetFunction", "lambda:GetFunctionConfiguration"],
         "Resource": f"arn:aws:lambda:{REGION}:{account}:function:{prefix}*"},
        # RegisterTaskDefinition does not support a resource ARN condition.
        {"Effect": "Allow", "Action": ["ecs:RegisterTaskDefinition"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["ecs:TagResource"],
         "Resource": f"arn:aws:ecs:{REGION}:{account}:task-definition/{prefix}*:*"},
        # Exact numeric versions: no automatic retry; completion/failure records
        # go to the pipeline notification queue (hints, never publication).
        {"Effect": "Allow", "Action": ["lambda:PutFunctionEventInvokeConfig", "lambda:GetFunctionEventInvokeConfig"],
         "Resource": f"arn:aws:lambda:{REGION}:{account}:function:{prefix}*:*"},
    ]
    for approved in policy.get("private_wheels", {}).values():
        private_bucket, key = approved["s3_uri"][5:].split("/", 1)
        statements.append({
            "Effect": "Allow", "Action": ["s3:GetObjectVersion"],
            "Resource": f"arn:aws:s3:::{private_bucket}/{key}",
            "Condition": {"StringEquals": {"s3:VersionId": approved["object_version"]}}})
    resources = {
        "BuildLogs": {"Type": "AWS::Logs::LogGroup",
                      "Properties": {"RetentionInDays": 7, "Tags": tags}},
        "RuntimeLogs": {"Type": "AWS::Logs::LogGroup",
                        "Properties": {"RetentionInDays": 7, "Tags": tags}},
        "ExecutionRole": {"Type": "AWS::IAM::Role", "Properties": {
            "Tags": tags,
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                "Action": "sts:AssumeRole"}]},
            "Policies": [{"PolicyName": "EnvironmentImageAndLogs", "PolicyDocument": {
                "Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": ["ecr:GetAuthorizationToken"], "Resource": "*"},
                    {"Effect": "Allow", "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer",
                                                 "ecr:BatchCheckLayerAvailability"],
                     "Resource": repository_arn},
                    {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                     "Resource": {"Fn::Sub":
                         f"arn:aws:logs:{REGION}:{account}:log-group:${{RuntimeLogs}}:log-stream:*"}},
                ]}}]}},
        "PipelineEvents": {"Type": "AWS::SQS::Queue", "Properties": {
            "SqsManagedSseEnabled": True, "MessageRetentionPeriod": 345600, "VisibilityTimeout": 60,
            "Tags": tags}},
        "PipelineEventsPolicy": {"Type": "AWS::SQS::QueuePolicy", "Properties": {
            "Queues": [{"Ref": "PipelineEvents"}],
            "PolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "events.amazonaws.com"}, "Action": "sqs:SendMessage",
                "Resource": {"Fn::GetAtt": ["PipelineEvents", "Arn"]},
                "Condition": {"ArnEquals": {"aws:SourceArn": {"Fn::GetAtt": ["ComputeStopped", "Arn"]}}}}]}}},
        # Independent compute tasks stopping in the shared cluster; the
        # scheduler still decides from exit code plus staged completion.
        "ComputeStopped": {"Type": "AWS::Events::Rule", "Properties": {
            "State": "ENABLED", "EventPattern": {
                "source": ["aws.ecs"], "detail-type": ["ECS Task State Change"],
                "detail": {"clusterArn": [cluster_arn], "lastStatus": ["STOPPED"],
                           "startedBy": [{"prefix": "colvenna-"}]}},
            "Targets": [{"Id": "pipeline-events", "Arn": {"Fn::GetAtt": ["PipelineEvents", "Arn"]}}]}},
        "RuntimeBoundary": {"Type": "AWS::IAM::ManagedPolicy", "Properties": {
            "Description": "Colvenna Isle user execution: generation inputs/staging and runtime logs only",
            "PolicyDocument": {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": ["s3:GetObjectVersion"], "Resource": runtime_files},
                {"Effect": "Allow", "Action": ["s3:PutObject"],
                 "Resource": f"arn:aws:s3:::{bucket}/spaces/*/staging/gen_*/*"},
                # Lambda delivers the destination record with the function role.
                {"Effect": "Allow", "Action": ["sqs:SendMessage"],
                 "Resource": {"Fn::GetAtt": ["PipelineEvents", "Arn"]}},
                {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                 "Resource": {"Fn::Sub":
                     f"arn:aws:logs:{REGION}:{account}:log-group:${{RuntimeLogs}}:log-stream:*"}},
            ]}}},
        "BuildRole": {"Type": "AWS::IAM::Role", "Properties": {
            "Tags": tags,
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "codebuild.amazonaws.com"},
                "Action": "sts:AssumeRole", "Condition": {"StringEquals": {
                    "aws:SourceAccount": account,
                    "aws:SourceArn": f"arn:aws:codebuild:{REGION}:{account}:project/{project}"}}}]},
            "Policies": [{"PolicyName": "TrustedBuilder",
                          "PolicyDocument": {"Version": "2012-10-17", "Statement": statements}}]}},
        "Builder": {"Type": "AWS::CodeBuild::Project", "Properties": {
            "Name": project, "Tags": tags, "ServiceRole": {"Fn::GetAtt": ["BuildRole", "Arn"]},
            "TimeoutInMinutes": 30, "QueuedTimeoutInMinutes": 15, "ConcurrentBuildLimit": 4,
            "Artifacts": {"Type": "NO_ARTIFACTS"},
            "Source": {"Type": "S3", "Location": bucket + "/" + source["key"],
                       "BuildSpec": "buildspec.yml"},
            "SourceVersion": source["version_id"],
            "Environment": {"Type": "ARM_CONTAINER", "ComputeType": "BUILD_GENERAL1_SMALL",
                            "Image": policy["builder_image"], "ImagePullCredentialsType": "CODEBUILD",
                            "PrivilegedMode": True,
                            "EnvironmentVariables": [{"Name": "COLVENNA_ACCOUNT", "Value": account, "Type": "PLAINTEXT"},
                                {"Name": "COLVENNA_REGION", "Value": {"Ref": "AWS::Region"}, "Type": "PLAINTEXT"},
                                {"Name": "AWS_DEFAULT_REGION", "Value": {"Ref": "AWS::Region"}, "Type": "PLAINTEXT"}]},
            "LogsConfig": {"CloudWatchLogs": {"Status": "ENABLED", "GroupName": {"Ref": "BuildLogs"}}},
        }},
    }
    return regional({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Colvenna Isle immutable Python environments",
        "Resources": resources,
        "Outputs": {
            "Project": {"Value": {"Ref": "Builder"}},
            "RuntimeLogGroup": {"Value": {"Ref": "RuntimeLogs"}},
            "RuntimeBoundaryArn": {"Value": {"Ref": "RuntimeBoundary"}},
            "BuildRoleArn": {"Value": {"Fn::GetAtt": ["BuildRole", "Arn"]}},
            "ExecutionRoleArn": {"Value": {"Fn::GetAtt": ["ExecutionRole", "Arn"]}},
            "PipelineEventsQueueArn": {"Value": {"Fn::GetAtt": ["PipelineEvents", "Arn"]}},
            "PipelineEventsQueueUrl": {"Value": {"Ref": "PipelineEvents"}},
            "SourceCommit": {"Value": policy["source_commit"]},
            "BaselineStackId": {"Value": baseline["StackId"]},
        },
    })
