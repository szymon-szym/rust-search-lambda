import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

export class RustSearchStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // define vpc without nat gateways
    const vpc = new cdk.aws_ec2.Vpc(this, 'Search VPC', {
      maxAzs: 2,
      natGateways: 0,
      gatewayEndpoints: {
        S3: {
          service: ec2.GatewayVpcEndpointAwsService.S3,
        }
      }
    })

    // s3 bucket for posts
    const postsBucket = new cdk.aws_s3.Bucket(this, 'BucketPosts', {
      removalPolicy: cdk.RemovalPolicy.DESTROY
    })

    // define elastic file system to be used by lambda
    const fs = new cdk.aws_efs.FileSystem(this, 'FileSystem', {
      vpc,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      encrypted: false
    })

    const accessPoint = fs.addAccessPoint('SearchAccessPoint', {
      path: '/lambda',
      createAcl: {
        ownerGid: '1001',
        ownerUid: '1001',
        permissions: '750'
      },
      posixUser: {
        uid: '1001',
        gid: '1001'
      }
    })

    //lambda searcher - rust project
    const searcher = new cdk.aws_lambda.Function(this, 'Searcher', {
      code: cdk.aws_lambda.Code.fromAsset('./lambdas/searcher/target/lambda/searcher'),
      runtime: cdk.aws_lambda.Runtime.PROVIDED_AL2,
      handler: 'does_not_matter',
      vpc,
      filesystem: cdk.aws_lambda.FileSystem.fromEfsAccessPoint(accessPoint, '/mnt/lambda'),
      environment: {
        'RUST_BACKTRACE': '1',
        'PATH_EFS': '/mnt/lambda'
      }})

      //lambda indexer - rust project
    const indexer = new cdk.aws_lambda.Function(this, 'Indexer', {
      code: cdk.aws_lambda.Code.fromAsset('./lambdas/indexer/target/lambda/indexer'),
      runtime: cdk.aws_lambda.Runtime.PROVIDED_AL2,
      handler: 'does_not_matter',
      vpc,
      filesystem: cdk.aws_lambda.FileSystem.fromEfsAccessPoint(accessPoint, '/mnt/lambda'),
      environment: {
        'RUST_BACKTRACE': '1',
        'PATH_EFS': '/mnt/lambda',
        'POSTS_BUCKET_NAME': postsBucket.bucketName
      }})

      postsBucket.grantRead(indexer)
  }
}
