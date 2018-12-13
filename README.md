# Fraud detection in credit card payments and auto insurance claims using PySpark
In this repo I trained baseline classifiers for different fraud detection tasks. The fist task was credit card fraud detection on the dataset called [Paysim](https://github.com/EdgarLopezPhD/PaySim). In addition, two popular auto insurance fraud detection datasets were analyzed: the example of 1000 samples used in a [Databricks notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4954928053318020/1058911316420443/167703932442645/latest.html) and the [Oracle's example](https://blogs.oracle.com/datamining/fraud-and-anomaly-detection-made-simple) for outlier detection containing 14700 samples. This latter dataset has been also used in some scientific work ([DeBarr, et al.][1]; [Nian, et al.][2]), and the details of its features are described in some detail by [Phua, et al.][3], who shared the dataset at his [web site](http://clifton.phua.googlepages.com/minority-report-data.zip).  

The examples I share in this repo use PySpark and they are prepared for large CSV file processing in standalone mode. In particular, the use of the `spark.ml` module was favored as the RDD-based MLLIB library is going to be deprecated.

The Databricks dataset is mainly focused on the classification of the claims while the Oracle dataset is mainly focused on the classification of policy holders with fraudulent behavior. This makes a very interesting ensemble of features.

[1]: http://worldcomp-proceedings.com/proc/p2013/DMI8055.pdf "DeBarr, D., & Wechsler, H. (2013, January). Fraud detection using reputation features, SVMs, and random forests. In Proceedings of the International Conference on Data Mining (DMIN) (p. 1). The Steering Committee of The World Congress in Computer Science, Computer Engineering and Applied Computing (WorldComp)."

[2]: https://doi.org/10.1016/j.jfds.2016.03.001 "Nian, K., Zhang, H., Tayal, A., Coleman, T., & Li, Y. (2016). Auto insurance fraud detection using unsupervised spectral ranking for anomaly. The Journal of Finance and Data Science, 2(1), 58-75."

[3]: https://sites.google.com/site/cliftonphua/minority-report.pdf "Phua, C., Alahakoon, D., & Lee, V. (2004). Minority report in fraud detection: classification of skewed data. Acm sigkdd explorations newsletter, 6(1), 50-59."
