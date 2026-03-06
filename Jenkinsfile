pipeline {
    agent any

    // ─────────────────────────────────────────────
    // Runtime parameters (editable per-run in UI)
    // ─────────────────────────────────────────────
    // ─────────────────────────────────────────────
    // Automatic triggers
    //   githubPush()  → fires on every push via GitHub webhook
    //   pollSCM       → fallback polling every 2 min if webhook is unreachable
    // ─────────────────────────────────────────────
    triggers {
        githubPush()
        pollSCM('H/2 * * * *')
    }

    parameters {
        booleanParam(
            name: 'PUSH_IMAGES',
            defaultValue: false,
            description: 'Push tagged Docker images to the registry after build'
        )
        string(
            name: 'DOCKER_REGISTRY',
            defaultValue: '',
            description: 'Registry prefix for image names, e.g. myregistry.io/hms (leave empty for local)'
        )
    }

    environment {
        // Jenkins credential IDs — configure in Jenkins → Manage Jenkins → Credentials
        GIT_CREDENTIALS_ID    = 'git-credentials'
        NEXUS_CREDENTIALS_ID  = 'nexus-credentials'

        // Nexus raw (hosted) repository endpoint
        // Repository name must be "artifacts" (create it in Nexus UI first)
        NEXUS_URL = 'http://nexus:8081'
        NEXUS_REPO = 'artifacts'

        // Image base names
        IMG_HIVEMETASTORE = 'hms/hivemetastore'
        IMG_SPARK         = 'hms/spark-master'
        IMG_AIRFLOW       = 'hms/airflow'
    }

    stages {

        // ─────────────────────────────────────────────
        // 1. CHECKOUT
        // ─────────────────────────────────────────────
        stage('Checkout') {
            steps {
                checkout scm
                script {
                    env.GIT_COMMIT_SHORT = sh(
                        script: 'git rev-parse --short HEAD',
                        returnStdout: true
                    ).trim()
                    echo "Commit: ${env.GIT_COMMIT_SHORT}"
                }
            }
        }

        // ─────────────────────────────────────────────
        // 2. GENERATE VERSION
        //    Reads base version from VERSION file and
        //    appends the Jenkins build number.
        //    Result: v1.0.0-build.42
        // ─────────────────────────────────────────────
        stage('Generate Version') {
            steps {
                script {
                    def baseVersion = readFile('VERSION').trim()
                    env.VERSION_TAG  = "v${baseVersion}-build.${BUILD_NUMBER}"
                    env.BASE_VERSION = baseVersion

                    // Resolve full image names (with optional registry prefix)
                    def prefix = params.DOCKER_REGISTRY ? "${params.DOCKER_REGISTRY}/" : ""
                    env.FULL_IMG_HIVEMETASTORE = "${prefix}${IMG_HIVEMETASTORE}"
                    env.FULL_IMG_SPARK         = "${prefix}${IMG_SPARK}"
                    env.FULL_IMG_AIRFLOW        = "${prefix}${IMG_AIRFLOW}"

                    echo "Version tag : ${env.VERSION_TAG}"
                    echo "Images      : ${env.FULL_IMG_HIVEMETASTORE}, ${env.FULL_IMG_SPARK}, ${env.FULL_IMG_AIRFLOW}"

                    // Write build info artifact
                    writeFile file: 'build-info.txt', text: """\
BUILD INFO
==========
Version    : ${env.VERSION_TAG}
Base       : ${env.BASE_VERSION}
Build #    : ${BUILD_NUMBER}
Commit     : ${env.GIT_COMMIT_SHORT}
Branch     : ${env.BRANCH_NAME ?: 'N/A'}
Timestamp  : ${new Date()}
Images:
  - ${env.FULL_IMG_HIVEMETASTORE}:${env.VERSION_TAG}
  - ${env.FULL_IMG_SPARK}:${env.VERSION_TAG}
  - ${env.FULL_IMG_AIRFLOW}:${env.VERSION_TAG}
"""
                }
            }
        }

        // ─────────────────────────────────────────────
        // 3. BUILD DOCKER IMAGES (parallel)
        // ─────────────────────────────────────────────
        stage('Build Docker Images') {
            parallel {

                stage('Build hivemetastore') {
                    steps {
                        sh """
                            docker build \\
                                -f docker/Dockerfile \\
                                -t ${env.FULL_IMG_HIVEMETASTORE}:${env.VERSION_TAG} \\
                                --label git-commit=${env.GIT_COMMIT_SHORT} \\
                                --label build-number=${BUILD_NUMBER} \\
                                .
                        """
                    }
                }

                stage('Build spark-master') {
                    steps {
                        sh """
                            docker build \\
                                -t ${env.FULL_IMG_SPARK}:${env.VERSION_TAG} \\
                                --label git-commit=${env.GIT_COMMIT_SHORT} \\
                                --label build-number=${BUILD_NUMBER} \\
                                ./spark
                        """
                    }
                }

                stage('Build airflow') {
                    steps {
                        sh """
                            docker build \\
                                -t ${env.FULL_IMG_AIRFLOW}:${env.VERSION_TAG} \\
                                --label git-commit=${env.GIT_COMMIT_SHORT} \\
                                --label build-number=${BUILD_NUMBER} \\
                                ./airflow
                        """
                    }
                }
            }
        }

        // ─────────────────────────────────────────────
        // 4. TAG IMAGES AS :latest
        // ─────────────────────────────────────────────
        stage('Tag Images') {
            steps {
                sh """
                    docker tag ${env.FULL_IMG_HIVEMETASTORE}:${env.VERSION_TAG} ${env.FULL_IMG_HIVEMETASTORE}:latest
                    docker tag ${env.FULL_IMG_SPARK}:${env.VERSION_TAG}         ${env.FULL_IMG_SPARK}:latest
                    docker tag ${env.FULL_IMG_AIRFLOW}:${env.VERSION_TAG}        ${env.FULL_IMG_AIRFLOW}:latest
                """
                echo "Tagged all images as :${env.VERSION_TAG} and :latest"
            }
        }

        // ─────────────────────────────────────────────
        // 5. PUSH IMAGES (optional, off by default)
        // ─────────────────────────────────────────────
        stage('Push Images') {
            when {
                expression { return params.PUSH_IMAGES }
            }
            steps {
                withCredentials([usernamePassword(
                    credentialsId: 'docker-registry-credentials',
                    usernameVariable: 'DOCKER_USER',
                    passwordVariable: 'DOCKER_PASS'
                )]) {
                    sh """
                        echo "$DOCKER_PASS" | docker login ${params.DOCKER_REGISTRY} -u "$DOCKER_USER" --password-stdin

                        docker push ${env.FULL_IMG_HIVEMETASTORE}:${env.VERSION_TAG}
                        docker push ${env.FULL_IMG_HIVEMETASTORE}:latest

                        docker push ${env.FULL_IMG_SPARK}:${env.VERSION_TAG}
                        docker push ${env.FULL_IMG_SPARK}:latest

                        docker push ${env.FULL_IMG_AIRFLOW}:${env.VERSION_TAG}
                        docker push ${env.FULL_IMG_AIRFLOW}:latest

                        docker logout ${params.DOCKER_REGISTRY}
                    """
                }
            }
        }

        // ─────────────────────────────────────────────
        // 6. GIT TAG
        //    Creates an annotated tag and pushes it.
        //    Requires 'git-credentials' in Jenkins.
        // ─────────────────────────────────────────────
        stage('Git Tag') {
            steps {
                withCredentials([usernamePassword(
                    credentialsId: env.GIT_CREDENTIALS_ID,
                    usernameVariable: 'GIT_USER',
                    passwordVariable: 'GIT_PASS'
                )]) {
                    sh """
                        git config user.email "jenkins@ci.local"
                        git config user.name  "Jenkins"

                        # Delete local tag if it already exists (re-run safety)
                        git tag -d ${env.VERSION_TAG} 2>/dev/null || true

                        git tag -a ${env.VERSION_TAG} \\
                            -m "Jenkins build #${BUILD_NUMBER} — ${env.VERSION_TAG} (commit: ${env.GIT_COMMIT_SHORT})"

                        git push https://${GIT_USER}:${GIT_PASS}@\$(git remote get-url origin | sed 's|https://||') \\
                            ${env.VERSION_TAG}
                    """
                }
                echo "Git tag ${env.VERSION_TAG} pushed successfully"
            }
        }

        // ─────────────────────────────────────────────
        // 7. PUBLISH ARTIFACT TO NEXUS
        //    Uploads build-info.txt to the Nexus raw
        //    (hosted) repository named "artifacts".
        //    Path in Nexus:
        //      artifacts/<job-name>/<version>/build-info.txt
        //
        //    Browse at:
        //      http://localhost:8082/#browse/browse:artifacts
        // ─────────────────────────────────────────────
        stage('Publish to Nexus') {
            steps {
                withCredentials([usernamePassword(
                    credentialsId: env.NEXUS_CREDENTIALS_ID,
                    usernameVariable: 'NEXUS_USER',
                    passwordVariable: 'NEXUS_PASS'
                )]) {
                    sh """
                        curl -f -s -u "${NEXUS_USER}:${NEXUS_PASS}" \\
                            -X POST \\
                            -H "Content-Type: multipart/form-data" \\
                            -F "raw.directory=/${JOB_NAME}/${env.VERSION_TAG}/" \\
                            -F "raw.asset1=@build-info.txt" \\
                            -F "raw.asset1.filename=build-info.txt" \\
                            "${env.NEXUS_URL}/service/rest/v1/components?repository=${env.NEXUS_REPO}"

                        echo "Artifact uploaded to: ${env.NEXUS_URL}/repository/${env.NEXUS_REPO}/${JOB_NAME}/${env.VERSION_TAG}/build-info.txt"
                    """
                }
            }
        }
    }

    // ─────────────────────────────────────────────
    // POST-BUILD NOTIFICATIONS
    // ─────────────────────────────────────────────
    post {
        success {
            echo """
╔══════════════════════════════════════════════╗
║  BUILD SUCCESS                               ║
║  Version : ${env.VERSION_TAG}
║  Images  : tagged & ready                   ║
╚══════════════════════════════════════════════╝
"""
        }
        failure {
            echo "BUILD FAILED — version ${env.VERSION_TAG} was NOT tagged or pushed."
        }
        always {
            // Clean up dangling build images to save disk space
            sh """
                docker image prune -f --filter label=build-number=${BUILD_NUMBER} || true
            """
        }
    }
}
