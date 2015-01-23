namespace java ranpanf.thrift
exception HBaseClusterSwitchException{
}
service HBaseHubService{
    list<binary> getClusters();
    binary getMasterCluster();
    list<binary> getSlaveClusters();
    bool shift(1:binary cluster) throws(1:HBaseClusterSwitchException ex)
}
