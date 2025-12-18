module "s3" {
  source  = "../../modules/s3"
  project = var.project
  env     = var.env
}

module "ecr" {
  source  = "../../modules/ecr"
  project = var.project
  env     = var.env
}

module "github_oidc" {
  source      = "../../modules/github_oidc"
  project     = var.project
  env         = var.env
  github_repo = var.github_repo
}
