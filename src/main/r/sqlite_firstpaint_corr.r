rawdata <- read.csv("~/Desktop/telemetry-sqlite.csv")
fppl<-rawdata[rawdata$version=="10.0a1" & rawdata$os=="WINNT" & rawdata$osversion=="6.1" & !is.na(rawdata$firstpaint) & !is.na(rawdata$places),]
fpot<-rawdata[rawdata$version=="10.0a1" & rawdata$os=="WINNT" & rawdata$osversion=="6.1" & !is.na(rawdata$firstpaint) & !is.na(rawdata$other),]
plot(fppl$firstpaint,fppl$places)
#plot(fpot$firstpaint,fpot$other)

cor(fppl$firstpaint,fppl$places,method="pearson")
cor(fppl$firstpaint,fppl$places,method="spearman")

cor(fpot$firstpaint,fpot$other,method="pearson")
cor(fpot$firstpaint,fpot$other,method="spearman")


rawdata<-read.csv("~/Desktop/telemetry-ndco.csv")
data<-rawdata[rawdata$version=="10.0a1" & rawdata$os=="WINNT" & !is.na(rawdata$firstpaint) & !is.na(rawdata$ndco),]
data$firstpaint[data$firstpaint > 600000]<-605000
data$ndco[data$ndco > 30000]<-31000
hist(data$firstpaint, breaks=c(seq(60000,600000,by=5000),max(data$firstpaint)), freq=FALSE, xlab="Time Buckets (1000ms intervals)", main="Telemetry firstPaint > 30s", col=rgb(0,0,1,0.5))
hist(data$ndco, breaks=c(seq(0,30000,by=1000),max(data$ndco)), freq=TRUE, xlab="Time Buckets (1000ms intervals)", main="Telemetry NETWORK_DISK_CACHE_OPEN (firstPaint > 30s)", col=rgb(0,0,1,0.5))

fpmin<-min(data$firstpaint)
fpmax<-max(data$firstpaint)
fpscaled<-as.numeric(lapply(data$firstpaint,function(x) (x-fpmin)/(fpmax-fpmin)))

ncmin<-min(data$ndco)
ncmax<-max(data$ndco)
ncscaled<-as.numeric(lapply(data$ndco,function(x) (x-ncmin)/(ncmax-ncmin)))