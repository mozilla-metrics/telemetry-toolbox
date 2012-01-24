library(ggplot2)

nRowsPlotted <- 50
dat <- read.csv(file.choose(), sep='\t')
names(dat) <- c('sql', 'count', 'time')
dat$avg.time <- dat$time / dat$count
dat2 <- ddply(dat, .(sql), function(x) {
        avg.time <- mean(x$avg.time)
        count <- sum(x$count)
        data.frame(count=count, avg.time=avg.time)
})
dat2$idx <- 1:nrow(dat2)
qplot(avg.time, reorder(idx, avg.time), data=head(dat2, nRowsPlotted), geom='point', size=count) +
          scale_area() +
          scale_x_log10() +
          ylab('SQL') +
          xlab('Average time in ms')