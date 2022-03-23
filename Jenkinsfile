properties([
    buildDiscarder(logRotator(daysToKeepStr: '30', numToKeepStr: '10')),
])
currentBuild.rawBuild.getParent().setQuietPeriod(0)


library 'jenkins-pipeline-library@main'

node {
  notify(slackChannel: '#re-build') {
    stage ('Checkout') {
      checkout scm
    }
    stage('Build') {
      deployer.code()
    }
  }
}
