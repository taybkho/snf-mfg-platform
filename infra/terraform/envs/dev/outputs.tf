output "raw_bucket_name" { value = module.s3.raw_bucket_name }
output "artifacts_bucket_name" { value = module.s3.artifacts_bucket_name }
output "ecr_repo_url" { value = module.ecr.repo_url }
output "github_actions_role_arn" { value = module.github_oidc.role_arn }
