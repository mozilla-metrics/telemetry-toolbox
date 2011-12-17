slowsql_main<-read.delim("~/workspace/telemetry-toolbox/data/telemetry-slowsql-main.tsv", header=F, col.names=c("sql","count","time"))
slowsql_main$count<-as.numeric(slowsql_main$count)
slowsql_main$time<-as.numeric(slowsql_main$time)
slowsql_main$avg_time<-(slowsql_main$time / slowsql_main$count)

main_count_agg<-aggregate(slowsql_main$count, by=list(slowsql_main$sql), FUN=sum)
colnames(main_count_agg)<-c("sql","count")
main_time_agg<-aggregate(slowsql_main$avg_time, by=list(slowsql_main$sql), FUN=mean)
colnames(main_time_agg)<-c("sql","time")
main_merged<-merge(main_count_agg, main_time_agg, by.x="sql", by.y="sql", all=TRUE)
main_merged.bycount<-main_merged[order(main_merged$count, decreasing=TRUE),]
main_merged.bytime<-main_merged[order(main_merged$time, decreasing=TRUE),]
write.table(main_merged.bytime, file="~/workspace/telemetry-toolbox/data/main-bytime.txt", row.names=FALSE, sep="\t")
write.table(main_merged.bycount, file="~/workspace/telemetry-toolbox/data/main-bycount.txt", row.names=FALSE, sep="\t")

slowsql_other<-read.delim("~/workspace/telemetry-toolbox/data/telemetry-slowsql-other.tsv", header=F, col.names=c("sql","count","time"))
slowsql_other$count<-as.numeric(slowsql_other$count)
slowsql_other$time<-as.numeric(slowsql_other$time)
slowsql_other$avg_time<-(slowsql_other$time / slowsql_other$count)

other_count_agg<-aggregate(slowsql_other$count, by=list(slowsql_other$sql), FUN=sum)
colnames(other_count_agg)<-c("sql","count")
other_time_agg<-aggregate(slowsql_other$avg_time, by=list(slowsql_other$sql), FUN=mean)
colnames(other_time_agg)<-c("sql","time")
other_merged<-merge(other_count_agg, other_time_agg, by.x="sql", by.y="sql", all=TRUE)
other_merged.bycount<-other_merged[order(other_merged$count, decreasing=TRUE),]
other_merged.bytime<-other_merged[order(other_merged$time, decreasing=TRUE),]
write.table(other_merged.bytime, file="~/workspace/telemetry-toolbox/data/other-bytime.txt", row.names=FALSE, sep="\t")
write.table(other_merged.bycount, file="~/workspace/telemetry-toolbox/data/other-bycount.txt", row.names=FALSE, sep="\t")
