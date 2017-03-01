Infinispan Playground Hybrid Map/Reduce Word Count demo
=======================================================

This package demonstrates how to run map/reduce jobs in a hybrid Infinispan 
cluster. A hybrid cluster is one where nodes can be a mix of embedded nodes 
(i.e. nodes where Infinispan is embedded as a library) and server nodes. 

To use this demo you will need to have already installed an instance of
Infinispan Server.

Build and package the demo:

$ mvn clean package

This will compile the classes and generate in the target directory a server 
module packaged as a zip file which you need to extract at the root of the 
server.
You will also need to modify the following file:

  modules/system/layers/base/org/jboss/as/clustering/infinispan/main/module.xml 

by adding the following line to its dependencies:

  <module name="net.dataforte.infinispan.playground.hybrid"/>

Start the server using the following:

$ ./bin/standalone.sh -c server-hybrid.xml

Now run the embedded node as follows:

$ mvn exec:java

