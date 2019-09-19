FROM bigcontainer/flume
ENTRYPOINT ["bin/flume-ng", "agent"]
ADD flume.conf /opt/flume/flume.conf
CMD ["-n", "agent", "-c", "conf", "-f", "flume.conf", "-Dflume.root.logger=INFO,console"]