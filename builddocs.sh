#set -e

# figure out the Java version (Oracle JDK and OpenJDK compatible)
JAVA_VERSION=$(java -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

VERS=$(java -version 2>&1)
echo "Detected Java version: $JAVA_VERSION from string $VERS"

# Java 8 and newer needs this fix
if [ "$JAVA_VERSION" -ge 18 ] ; then
    JAVA8_DOC_FIX='-DadditionalJOption="-Xdoclint:none"'
    echo "setting flag"
else
    JAVA8_DOC_FIX=''
fi

JAVA8_DOC_FIX='-DadditionalJOption="-Xdoclint:none"'

# create api dir for scala and java docs
mkdir -p docs/target/api

# get maven
wget https://archive.apache.org/dist/maven/maven-3/3.2.5/binaries/apache-maven-3.2.5-bin.tar.gz
tar xf apache-maven-3.2.5-bin.tar.gz
MVN=`pwd`/apache-maven-3.2.5/bin/mvn

# build java docs
$MVN -version
javadoc -J-version
echo "java8 fix: $JAVA8_DOC_FIX"
$MVN javadoc:aggregate -Paggregate-scaladoc -DadditionalJOption="-Xdoclint:none" -Dmaven.javadoc.failOnError=false -Dheader="<a href=\"http://flink.apache.org/\" target=\"_top\"><h1>Back to Flink Website</h1></a>"

# move java docs
mv target/site/apidocs docs/target/api/java

# build scala docs
cd stratosphere-scala || cd flink-scala 
$MVN scala:doc

# move scala docs
mv target/site/scaladocs ../docs/target/api/scala
