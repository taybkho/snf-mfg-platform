variable "project" { type = string }
variable "env"     { type = string }

resource "aws_ecr_repository" "repo" {
  name = "${var.project}-${var.env}-dq-service"
  image_scanning_configuration { scan_on_push = true }
}

output "repo_url" { value = aws_ecr_repository.repo.repository_url }
