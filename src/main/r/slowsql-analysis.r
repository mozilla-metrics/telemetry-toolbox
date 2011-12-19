slowsql_main<-read.delim("~/workspace/telemetry-toolbox/data/slowsql-main.tsv", header=F, col.names=c("sql","count","time"))
slowsql_main$count<-as.numeric(slowsql_main$count)
slowsql_main$time<-as.numeric(slowsql_main$time)
slowsql_main$avg_time<-(slowsql_main$time / slowsql_main$count)
slowsql_main.bytime<-slowsql_main[order(slowsql_main$time, decreasing=TRUE),]
write.table(slowsql_main.bytime, file="~/workspace/telemetry-toolbox/data/slowsql-main-bytime.txt", row.names=FALSE, sep="\t")

main_count_agg<-aggregate(slowsql_main$count, by=list(slowsql_main$sql), FUN=sum)
colnames(main_count_agg)<-c("sql","count")
main_time_agg<-aggregate(slowsql_main$time, by=list(slowsql_main$sql), FUN=sum)
colnames(main_time_agg)<-c("sql","time")
main_avg_time_agg<-aggregate(slowsql_main$avg_time, by=list(slowsql_main$sql), FUN=mean)
colnames(main_avg_time_agg)<-c("sql","avg_avg_time")
main_median_time_agg<-aggregate(slowsql_main$avg_time, by=list(slowsql_main$sql), FUN=median)
colnames(main_median_time_agg)<-c("sql","median_avg_time")
main_sd_time_agg<-aggregate(slowsql_main$avg_time, by=list(slowsql_main$sql), FUN=sd)
colnames(main_sd_time_agg)<-c("sql","sd_avg_time")

main_merged<-merge(main_count_agg, main_time_agg, by.x="sql", by.y="sql", all=TRUE)
main_merged<-merge(main_merged, main_avg_time_agg, by.x="sql", by.y="sql", all=TRUE)
main_merged<-merge(main_merged, main_median_time_agg, by.x="sql", by.y="sql", all=TRUE)
main_merged<-merge(main_merged, main_sd_time_agg, by.x="sql", by.y="sql", all=TRUE)
main_merged$avg_time<-(main_merged$time / main_merged$count)

main_merged.bycount<-main_merged[order(main_merged$count, decreasing=TRUE),]
main_merged.bytime<-main_merged[order(main_merged$median_avg_time, decreasing=TRUE),]
write.table(main_merged.bytime, file="~/workspace/telemetry-toolbox/data/main-bytime.txt", row.names=FALSE, sep="\t")
write.table(main_merged.bycount, file="~/workspace/telemetry-toolbox/data/main-bycount.txt", row.names=FALSE, sep="\t")

slowsql_other<-read.delim("~/workspace/telemetry-toolbox/data/slowsql-other.tsv", header=F, col.names=c("sql","count","time"))
slowsql_other$count<-as.numeric(slowsql_other$count)
slowsql_other$time<-as.numeric(slowsql_other$time)
slowsql_other$avg_time<-(slowsql_other$time / slowsql_other$count)
slowsql_other.bytime<-slowsql_other[order(slowsql_other$time, decreasing=TRUE),]
write.table(slowsql_other.bytime, file="~/workspace/telemetry-toolbox/data/slowsql-other-bytime.txt", row.names=FALSE, sep="\t")

other_count_agg<-aggregate(slowsql_other$count, by=list(slowsql_other$sql), FUN=sum)
colnames(other_count_agg)<-c("sql","count")
other_time_agg<-aggregate(slowsql_other$time, by=list(slowsql_other$sql), FUN=sum)
colnames(other_time_agg)<-c("sql","time")
other_avg_time_agg<-aggregate(slowsql_other$avg_time, by=list(slowsql_other$sql), FUN=mean)
colnames(other_avg_time_agg)<-c("sql","avg_avg_time")
other_median_time_agg<-aggregate(slowsql_other$avg_time, by=list(slowsql_other$sql), FUN=median)
colnames(other_median_time_agg)<-c("sql","median_avg_time")
other_sd_time_agg<-aggregate(slowsql_other$avg_time, by=list(slowsql_other$sql), FUN=sd)
colnames(other_sd_time_agg)<-c("sql","sd_avg_time")

other_merged<-merge(other_count_agg, other_time_agg, by.x="sql", by.y="sql", all=TRUE)
other_merged<-merge(other_merged, other_avg_time_agg, by.x="sql", by.y="sql", all=TRUE)
other_merged<-merge(other_merged, other_median_time_agg, by.x="sql", by.y="sql", all=TRUE)
other_merged<-merge(other_merged, other_sd_time_agg, by.x="sql", by.y="sql", all=TRUE)
other_merged$avg_time<-(other_merged$time / other_merged$count)

other_merged.bycount<-other_merged[order(other_merged$count, decreasing=TRUE),]
other_merged.bytime<-other_merged[order(other_merged$median_avg_time, decreasing=TRUE),]
write.table(other_merged.bytime, file="~/workspace/telemetry-toolbox/data/other-bytime.txt", row.names=FALSE, sep="\t")
write.table(other_merged.bycount, file="~/workspace/telemetry-toolbox/data/other-bycount.txt", row.names=FALSE, sep="\t")
