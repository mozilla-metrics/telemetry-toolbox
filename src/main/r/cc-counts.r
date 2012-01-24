library(ggplot2)

raw_data <- read.delim("~/workspace/telemetry-toolbox/data/telemetry-cc.txt", sep='\t', header=F, col.names=c("id", "date_str", "version", "os", "hist_name", "hist_value", "hist_value_count", "hist_sum"))
raw_data$date <- sapply(as.character(raw_data$date_str), function(s) as.Date(s, "%Y%m%d"))
raw_data$hist_value <- as.numeric(raw_data$hist_value)
raw_data$hist_value_count <- as.numeric(raw_data$hist_value_count)
raw_data$hist_sum <- as.numeric(raw_data$hist_sum)
raw_data <- na.omit(raw_data)
raw_data$hist_value_sum <- (raw_data$hist_value * raw_data$hist_value_count)

by_id <- ddply(raw_data, .(id,date,os), function(x) {
  sum_count <- sum(x$hist_value_count)
  sum_time <- sum(x$hist_value_sum)
  avg_time <- sum_time/sum_count
  data.frame(sum_count=sum_count, sum_time=sum_time, avg_time=avg_time)
})

by_date <- ddply(by_id, .(date,os), function(x) {
  avg_time <- mean(x$avg_time)
  sum_count <- sum(x$sum_count) / 1000
  data.frame(avg_time=avg_time, sum_count=sum_count)
})

qplot(date, avg_time, data=by_id, geom=c("point","smooth"), colour=factor(os), alpha=I(0.35), main="Date vs. CYCLE_COLLECTOR Avg. Time", ylab="avg. time (ms)") + scale_x_date()
qplot(date, avg_time, data=by_date, geom=c("line","smooth"), colour=factor(os), main="Date vs. CYCLE_COLLECTOR Avg. of Avg. Time", ylab="avg. of avg. time (ms)") + scale_x_date()
qplot(date, sum_count, data=by_date, geom=c("line"), colour=factor(os), main="Date vs. CYCLE_COLLECTOR sum(count)", ylab="count (in thousands)") + scale_x_date()
qplot(sum_count, avg_time, data=by_id, geom=c("point","smooth"), colour=factor(os), alpha=I(0.35), main="CYCLE_COLLECTOR sum(count) vs. avg. time", xlab="sum(count)", ylab="avg. time (ms)") + scale_x_log10() + scale_y_log10()
