library(ggplot2)

raw_data <- read.delim("~/workspace/telemetry-toolbox/data/telemetry-submissions-20111201-20111220.txt", sep='\t', header=F, col.names=c("date_str", "product", "version", "count", "sum_size", "avg_size"))
raw_data$date <- sapply(as.character(raw_data$date_str), function(s) as.Date(s, "%Y%m%d"))
raw_data$version_major <- sapply(as.character(raw_data$version), function(s) as.numeric(strsplit(s, "\\.")[[1]][1]))
#raw_data$version_major <- as.numeric(gsub('\\..*', '', raw_data$version))
raw_data$avg_size_kb <- raw_data$avg_size / 1024
raw_data$sum_size_gb <- raw_data$sum_size / (1024 * 1024 * 1024)

firefox_data <- raw_data[raw_data$product == 'Firefox' & raw_data$version %in% c("7.0.1","8.0.1","9.0","10.0a2","11.0a1"),]
thunderbird_data <- raw_data[raw_data$product == 'Thunderbird',]
fennec_data <- raw_data[raw_data$product == 'Fennec',]

plt <- qplot(date, sum_size_gb, version_major, data=firefox_data, geom="point", colour=factor(version_major))
plt + scale_x_date()

plt <- qplot(date, avg_size_kb, version_major, data=firefox_data, geom="smooth", colour=factor(version_major))
plt + scale_x_date()

plt <- qplot(version_major, avg_size_kb, data=firefox_data, geom="point", colour=factor(version_major))
print(plt)

plt <- qplot(date, sum_size_gb, version_major, data=fennec_data, geom="smooth", colour=factor(version_major))
plt + scale_x_date()

plt <- qplot(date, avg_size_kb, version_major, data=fennec_data, geom="smooth", colour=factor(version_major))
plt + scale_x_date()
