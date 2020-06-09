# Spark App

## Spark application using Scala and SBT
Spark is written in Scala, application can use SBT build tool that is used to run tests and package your projects as JAR files.

Spark 2.4.x is written in Scala 2.12 and SBT 1.2.x uses Scala 2.12, so it’s best to stick with SBT 1.2.x when building Spark application.

Spark = 2.4.5
scalaVersion = 2.12.10
sbt.version = 1.2.8

### HTTP/HTTPS/FTP Proxy
If you are behind a proxy requiring authentication, your sbt script must also pass flags to set the http.proxyUser and http.proxyPassword properties for HTTP, ftp.proxyUser and ftp.proxyPassword properties for FTP, or https.proxyUser and https.proxyPassword properties for HTTPS.

IntelliJ SBT
![Alt text](./docs/intellij_proxy.png?raw=true "VM Properties")

SBT needs to be aware of the proxy to download packages. To set this proxy, use the file dropdown -> settings > Build Executions and Deployment -> SBT -> VM parameters

    -XX:MaxPermSize=384M
    -Dhttp.proxyHost=myproxy
    -Dhttp.proxyPort=4200
    -Dhttp.proxyUser=c795701
    -Dhttp.proxyPassword=password
    -Dhttps.proxyHost=myproxy 
    -Dhttps.proxyPort=4200
    -Dhttps.proxyUser=c795701
    -Dhttps.proxyPassword=password
    -Dsbt.gigahorse=false
    -Dsbt.repository.secure=false
    -Dsbt.insecureprotocol=true

VM parameters
![Alt text](./docs/sbt_vm_props.png?raw=true "VM Properties")