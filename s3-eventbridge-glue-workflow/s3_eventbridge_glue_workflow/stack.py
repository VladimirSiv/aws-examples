import jsii
from aws_cdk import Stack
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_events as events
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from constructs import Construct


class S3EventBridgeGlueWorkflowStack(Stack):

    def __init__(self, scope: Construct, _id: str, **kwargs) -> None:
        super().__init__(scope, _id, **kwargs)
        region = Stack.of(self).region
        account = Stack.of(self).account

        bucket = s3.Bucket(
            self,
            id="Bucket",
            access_control=s3.BucketAccessControl.PRIVATE,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        bucket.enable_event_bridge_notification()

        glue_workflow = glue.CfnWorkflow(
            self,
            "GlueWorkflow",
            description="Event Driven Glue Workflow",
            name="glue-workflow",
            max_concurrent_runs=1,
        )
        workflow_arn = f"arn:aws:glue:{region}:{account}:workflow/{glue_workflow.name}"

        glue_job = glue.CfnJob(
            self,
            "GlueJob",
            description="Glue job",
            name="glue-job",
            role="glue-role",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
            ),
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,
            )
        )
        glue_job.add_dependency(glue_workflow)

        glue_trigger = glue.CfnTrigger(
            self,
            "GlueTrigger",
            description="Event Glue Job Trigger",
            name="glue-event-trigger",
            type="EVENT",
            workflow_name=glue_workflow.name,
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=glue_job.name,
                )
            ],
            event_batching_condition=glue.CfnTrigger.EventBatchingConditionProperty(
                batch_size=100,
                batch_window=900,
            )
        )
        glue_trigger.add_dependency(glue_workflow)
        glue_trigger.add_dependency(glue_job)

        event_target_role = iam.Role(
            self,
            "EventTargetRole",
            description="Role that allows EventBridge to target Glue Workflow",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        )
        event_target_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[workflow_arn],
                actions=["glue:notifyEvent"],
            )
        )

        event_rule = events.Rule(
            self,
            "EventRule",
            description="Rule to match PutObject event in a bucket",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={"bucket": {"name": bucket.bucket_name}},
            )
        )

        @jsii.implements(events.IRuleTarget)
        class GlueWorkflowEventRuleTarget():
            def bind(self, rule, id=None):
                return events.RuleTargetConfig(
                    arn=workflow_arn,
                    role=event_target_role,
                )

        event_rule.add_target(GlueWorkflowEventRuleTarget())
