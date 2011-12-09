library(ggplot2)

rawdata <- read.csv("~/Desktop/data.txt", header=T)

data_0<-rawdata[rawdata$version=="10.0a1" & rawdata$os=="WINNT" & rawdata$osversion=="6.1" & rawdata$si==0 & !is.na(rawdata$firstpaint) & rawdata$firstpaint > 0,]
data_1<-rawdata[rawdata$version=="10.0a1" & rawdata$os=="WINNT" & rawdata$osversion=="6.1" & rawdata$si==1 & !is.na(rawdata$firstpaint) & rawdata$firstpaint > 0,]

firstpaint_0<-as.numeric(data_0$firstpaint)
firstpaint_1<-as.numeric(data_1$firstpaint)
summary(firstpaint_0)
summary(firstpaint_1)

sessionrestored_0<-as.numeric(data_0$sessionrestored)
sessionrestored_1<-as.numeric(data_1$sessionrestored)
summary(sessionrestored_0)
summary(sessionrestored_1)

firstpaint_0[is.na(firstpaint_0)]<--1
firstpaint_1[is.na(firstpaint_1)]<--1
sessionrestored_0[is.na(sessionrestored_0)]<--1
sessionrestored_1[is.na(sessionrestored_1)]<--1

firstpaint_0[firstpaint_0 < 0]<--1
firstpaint_0[firstpaint_0 > 30000]<-31000
firstpaint_1[firstpaint_1 < 0]<--1
firstpaint_1[firstpaint_1 > 30000]<-31000

sessionrestored_0[sessionrestored_0 < 0]<--1
sessionrestored_0[sessionrestored_0 > 30000]<-31000
sessionrestored_1[sessionrestored_1 < 0]<--1
sessionrestored_1[sessionrestored_1 > 30000]<-31000

hist(firstpaint_0, breaks=c(min(firstpaint_0),seq(0,30000,by=1000),max(firstpaint_0)), freq=FALSE, xlab="Time Buckets (1000ms intervals)", main="Telemetry firstPaint", col=rgb(0,0,1,0.5))
hist(firstpaint_1, breaks=c(min(firstpaint_1),seq(0,30000,by=1000),max(firstpaint_1)), freq=FALSE, xlab="Time Buckets (1000ms intervals)", main="Telemetry firstPaint", col=rgb(1,0,0,0.5), add=T)
#hist(sessionrestored_0, breaks=c(min(sessionrestored_0),seq(0,30000,by=1000),max(sessionrestored_0)), freq=TRUE, xlab="Time Buckets (1000ms intervals)", main="Telemetry sessionRestored", col="blue")

plot(ecdf(firstpaint_0), main="ECDF firstPaint", col="blue")
plot(ecdf(firstpaint_0), main="ECDF firstPaint", col=rgb(1,0,0,0.5), add=T)

d<-density(firstpaint_0)
plot(d)

#plot(firstpaint_0,sessionrestored_0,xlabel="First Paint",ylabel="Session Restored")
#abline(lm(firstpaint_0~sessionrestored_0), col="red")