pipeline {
  agent {
    node {
      label 'maven'
    }
  }
  stages {
    // GC1, GC2, GC3
    stage('License + Crypto + CVE checks') {
      steps {
        git(url: 'http://git.app.eng.bos.redhat.com/git/jboss-prod-core/gates.git/', branch: '1.1')
        load 'gates/aggregated_license-crypto-cve/RunLicenseCryptoCveGates.groovy'
      }
    }
    // GC4
    stage('Deliverables source code checks') {
      steps {
        load 'gates/aggregated_deliverables_srccode/RunDeliverablesSrcCodeGates.groovy'
      }
    }
    // GC5
    stage('Build-time dependencies checks') {
      steps {
        load 'gates/dependencies/RunGate.groovy'
      }
    }
    // GC6, GC7
    stage('Versions identifiers checks') {
      steps {
        load 'gates/aggregated_versions_identifiers/RunVersionsIdentifiersGates.groovy'
      }
    }
    // GC8
    stage('Version alignment analysis checks') {
      steps {
        load 'gates/versionsalignmentanalysis/RunGate.groovy'
      }
    }
    // GCJ1, GCJ2, GCJ3
    stage('JDK and jar signature checks') {
      steps {
        load 'gates/aggregated_jdk_jar/RunJDKJarGates.groovy'
      }
    }
    // GCM1
    stage('Maven Version checks') {
      steps {
        load 'gates/mavenversioning/RunGate.groovy'
      }
    }
    // GCM2
    stage('Maven Repository checks') {
      steps {
        load 'gates/aggregated_maven_repository/RunMavenRepositoryGates.groovy'
      }
    }
    // GCA1
    stage('Build tool Version checks') {
      steps {
        load 'gates/buildtoolversioning/RunGate.groovy'
      }
    }
    // GCD1, GCD2, GCD3, GCD4, GCD5, GCD6, GCD7, GCD8, GCD9, GCD10, GCD11
    stage('Docker checks') {
      steps {
        load 'gates/aggregated_docker/RunDockerGates.groovy'
      }
    }
    // GCR1, GCR2, GCR3, GCR4
    stage('RPM checks') {
      steps {
        load 'gates/aggregated_rpm/RunRpmGates.groovy'
      }
    }
    // GI1
    stage('Infrastructure checks') {
      steps {
        load 'gates/deliverablesbuildsystem/RunGate.groovy'
      }
    }
    // GR1, GR2, GR3, GR4
    stage('General RH requirements checks') {
      steps {
        load 'gates/aggregated_general_requirements/RunGeneralRequirementsGates.groovy'
      }
    }
    stage('Store results') {
      steps {
        load 'gates/common/StorePipelineResults.groovy'
      }
    }
  }
}
