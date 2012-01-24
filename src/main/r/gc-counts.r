library(ggplot2)

raw_data <- read.delim("~/workspace/telemetry-toolbox/data/telemetry-gc-uptime-ratio.txt", sep='\t', header=F, col.names=c("key", "date_str", "version", "sum_count", "uptime"))
raw_data$date <- sapply(as.character(raw_data$date_str), function(s) as.Date(s, "%Y%m%d"))
raw_data$ratio <- raw_data$sum_count / raw_data$uptime
raw_data <- na.omit(raw_data)

by_date<-ddply(raw_data, .(date,version), function(x) {
  sum_sum_count <- sum(x$sum_count)
  median_sum_count <- median(x$sum_count)
  avg_sum_count <- mean(x$sum_count)
  sd_sum_count <- sd(x$sum_count)
  data.frame(sum_sum_count=sum_sum_count, median_sum_count=median_sum_count, avg_sum_count=avg_sum_count, sd_sum_count=sd_sum_count)
})

by_date_ratio<-ddply(raw_data, .(date,version), function(x) {
  median_ratio <- median(x$ratio)
  avg_ratio <- mean(x$ratio)
  sd_ratio <- sd(x$ratio)
  data.frame(median_ratio=median_ratio, avg_ratio=avg_ratio, sd_ratio=sd_ratio)
})

plt <- qplot(date, sum_sum_count, data=by_date, geom=c("line","smooth"))
plt + scale_x_date()
plt <- qplot(date, median_sum_count, data=by_date, geom=c("line","smooth"))
plt + scale_x_date()
plt <- qplot(date, avg_sum_count, data=by_date, geom=c("line","smooth"))
plt + scale_x_date()
plt <- qplot(date, sd_sum_count, data=by_date, geom=c("line","smooth"))
plt + scale_x_date()

plt <- qplot(date, avg_sum_count, data=by_date, geom="pointrange", ymin=avg_sum_count - sd_sum_count, ymax=avg_sum_count + sd_sum_count)
plt + scale_x_date()

qplot(date, median_ratio, data=by_date_ratio, geom=c("line","smooth"), colour=factor(version), main="Time vs. (GC_MS count / uptime) ratio", ylab="ratio") + 
  scale_x_date()