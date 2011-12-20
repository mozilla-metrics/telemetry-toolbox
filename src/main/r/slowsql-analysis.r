slowsql_main<-read.delim("~/workspace/telemetry-toolbox/data/slowsql-main.tsv", header=F, col.names=c("sql","count","time"))
slowsql_main$count<-as.numeric(slowsql_main$count)
slowsql_main$time<-as.numeric(slowsql_main$time)
slowsql_main$avg_time<-(slowsql_main$time / slowsql_main$count)
slowsql_main.bytime<-slowsql_main[order(slowsql_main$time, decreasing=TRUE),]
write.table(slowsql_main.bytime, file="~/workspace/telemetry-toolbox/data/slowsql-main-bytime.txt", row.names=FALSE, sep="\t")

main_data<-ddply(slowsql_main, .(sql), function(x) {
  count <- sum(x$count)
  sum_time <- sum(x$time)
  avg_time <- sum_time / count
  avg_avg_time <- mean(x$avg_time)
  median_avg_time <- median(x$avg_time)
  sd_avg_time <- sd(x$avg_time)
  data.frame(count=count, time=sum_time, avg_time=avg_time, avg_avg_time=avg_avg_time, median_avg_time=median_avg_time, sd_avg_time=sd_avg_time)
})

main_data.bycount<-main_data[order(main_data$count, decreasing=TRUE),]
main_data.bytime<-main_data[order(main_data$median_avg_time, decreasing=TRUE),]
write.table(main_data.bytime, file="~/workspace/telemetry-toolbox/data/main-bytime.txt", row.names=FALSE, sep="\t")
write.table(main_data.bycount, file="~/workspace/telemetry-toolbox/data/main-bycount.txt", row.names=FALSE, sep="\t")

slowsql_other<-read.delim("~/workspace/telemetry-toolbox/data/slowsql-other.tsv", header=F, col.names=c("sql","count","time"))
slowsql_other$count<-as.numeric(slowsql_other$count)
slowsql_other$time<-as.numeric(slowsql_other$time)
slowsql_other$avg_time<-(slowsql_other$time / slowsql_other$count)
slowsql_other.bytime<-slowsql_other[order(slowsql_other$time, decreasing=TRUE),]
write.table(slowsql_other.bytime, file="~/workspace/telemetry-toolbox/data/slowsql-other-bytime.txt", row.names=FALSE, sep="\t")

other_data<-ddply(slowsql_other, .(sql), function(x) {
  count <- sum(x$count)
  sum_time <- sum(x$time)
  avg_time <- sum_time / count
  avg_avg_time <- mean(x$avg_time)
  median_avg_time <- median(x$avg_time)
  sd_avg_time <- sd(x$avg_time)
  data.frame(count=count, time=sum_time, avg_time=avg_time, avg_avg_time=avg_avg_time, median_avg_time=median_avg_time, sd_avg_time=sd_avg_time)
})

other_data.bycount<-other_data[order(other_data$count, decreasing=TRUE),]
other_data.bytime<-other_data[order(other_data$median_avg_time, decreasing=TRUE),]
write.table(other_data.bytime, file="~/workspace/telemetry-toolbox/data/other-bytime.txt", row.names=FALSE, sep="\t")
write.table(other_data.bycount, file="~/workspace/telemetry-toolbox/data/other-bycount.txt", row.names=FALSE, sep="\t")
