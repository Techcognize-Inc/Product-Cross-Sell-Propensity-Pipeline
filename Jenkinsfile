pipeline {
  agent any

  options {
    timestamps()
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '25'))
  }

  parameters {
    booleanParam(name: 'RUN_DEPLOY', defaultValue: false, description: 'Run deployment after CI stages pass')
    string(name: 'DEPLOY_HOST', defaultValue: '', description: 'SSH target, example: ubuntu@10.0.0.15. Leave empty for local deploy on Jenkins node.')
    string(name: 'DEPLOY_PATH', defaultValue: '/opt/product-cross-sell', description: 'Path to repo on deploy target')
    string(name: 'DEPLOY_BRANCH', defaultValue: 'main', description: 'Branch to deploy')
    string(name: 'COMPOSE_FILE', defaultValue: 'docker-compose.yml', description: 'Compose file used for deployment')
    string(name: 'DEPLOY_STACK', defaultValue: 'airflow-webserver airflow-scheduler streamlit producer broadcast_publisher realtime_offer_writer', description: 'Services to rollout')
  }

  environment {
    PIP_DISABLE_PIP_VERSION_CHECK = '1'
    PYTHONUNBUFFERED = '1'
    CI_VENV_DIR = '.venv-ci'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Install Python Dependencies') {
      steps {
        script {
          if (isUnix()) {
            sh '''
              SYSTEM_PYTHON=$(command -v python3 || command -v python)
              "$SYSTEM_PYTHON" --version
              "$SYSTEM_PYTHON" -m venv "$CI_VENV_DIR"
              "$CI_VENV_DIR"/bin/python -m pip install --upgrade pip
              "$CI_VENV_DIR"/bin/python -m pip install -r requirements.txt
              "$CI_VENV_DIR"/bin/python -m pip install pytest
            '''
          } else {
            bat '''
              python --version
              python -m pip install --upgrade pip
              pip install -r requirements.txt
              pip install pytest
            '''
          }
        }
      }
    }

    stage('Validate DAG and Modules') {
      steps {
        script {
          if (isUnix()) {
            sh '''
              "$CI_VENV_DIR"/bin/python -m py_compile Airflow/dags/propensity_pipeline.py
              "$CI_VENV_DIR"/bin/python -m py_compile Batch/propensity_signals.py
              "$CI_VENV_DIR"/bin/python -m py_compile Flinkjobs/intent_detector.py
              "$CI_VENV_DIR"/bin/python -m py_compile Streamlit/app.py
            '''
          } else {
            bat '''
              python -m py_compile Airflow/dags/propensity_pipeline.py
              python -m py_compile Batch/propensity_signals.py
              python -m py_compile Flinkjobs/intent_detector.py
              python -m py_compile Streamlit/app.py
            '''
          }
        }
      }
    }

    stage('Unit Tests') {
      steps {
        script {
          if (isUnix()) {
            sh '''
              mkdir -p reports
              if [ -f tests/test_pipeline.py ]; then
                "$CI_VENV_DIR"/bin/python -m pytest tests/test_pipeline.py -q --maxfail=1 --disable-warnings --junitxml=reports/pytest.xml
              elif [ -d tests ]; then
                "$CI_VENV_DIR"/bin/python -m pytest tests -q --maxfail=1 --disable-warnings --junitxml=reports/pytest.xml
              else
                echo "No tests directory found. Skipping test stage."
              fi
            '''
          } else {
            bat """
              if not exist reports mkdir reports
              if exist tests\\test_pipeline.py (
                .venv-ci\\Scripts\\python -m pytest tests\\test_pipeline.py -q --maxfail=1 --disable-warnings --junitxml=reports\\pytest.xml
              ) else if exist tests (
                .venv-ci\\Scripts\\python -m pytest tests -q --maxfail=1 --disable-warnings --junitxml=reports\\pytest.xml
              ) else (
                echo No tests directory found. Skipping test stage.
              )
            """
          }
        }
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: 'reports/pytest.xml'
        }
      }
    }

    stage('Build Deployment Images') {
      steps {
        script {
          if (isUnix()) {
            sh '''
              docker compose -f docker-compose.yml build airflow-webserver airflow-scheduler streamlit producer broadcast_publisher realtime_offer_writer
            '''
          } else {
            bat '''
              docker compose -f docker-compose.yml build airflow-webserver airflow-scheduler streamlit producer broadcast_publisher realtime_offer_writer
            '''
          }
        }
      }
    }

    stage('Deploy') {
      when {
        expression { return params.RUN_DEPLOY }
      }
      steps {
        script {
          if (isUnix()) {
            sh 'chmod +x ci/scripts/deploy_compose.sh'
            if (params.DEPLOY_HOST?.trim()) {
              withCredentials([
                sshUserPrivateKey(credentialsId: 'deploy-ssh-key', keyFileVariable: 'SSH_KEY', usernameVariable: 'SSH_USER')
              ]) {
                sh '''
                  export GIT_SSH_COMMAND="ssh -i $SSH_KEY -o StrictHostKeyChecking=no"
                  ci/scripts/deploy_compose.sh "$DEPLOY_HOST" "$DEPLOY_PATH" "$COMPOSE_FILE" "$DEPLOY_STACK" "$DEPLOY_BRANCH"
                '''
              }
            } else {
              sh '''
                ci/scripts/deploy_compose.sh "" "$DEPLOY_PATH" "$COMPOSE_FILE" "$DEPLOY_STACK" "$DEPLOY_BRANCH"
              '''
            }
          } else {
            if (params.DEPLOY_HOST?.trim()) {
              error('Remote SSH deployment is supported on Unix Jenkins agents in this pipeline. Use a Linux agent or leave DEPLOY_HOST empty for local deploy.')
            }
            bat '''
              docker compose -f %COMPOSE_FILE% pull
              docker compose -f %COMPOSE_FILE% up -d --build %DEPLOY_STACK%
            '''
          }
        }
      }
    }
  }

  post {
    success {
      echo 'Pipeline completed successfully.'
    }
    failure {
      echo 'Pipeline failed. Check stage logs and test reports.'
    }
  }
}
