export CLASSPATH=$CLASSPATH:$(pwd)

java org.dejave.attica.server.Database --properties ../attica.properties $@
