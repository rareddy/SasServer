SAS Server
=========

A Translator for SAS Server to use in the JBoss Data Virtulization 6 platform. Even though SAS provides a JDBC driver, the JDBC driver does not provide metadata through DatabaseMetadata class. To full fill this gap a custom translator is needed to work with JDV6.

SAS provides system tables that contain the metadata, this translator is designed such that it uses these system tables to retrieve the metadata and convert them into metadata schema in the JDV system. This translator also defines the capabilities of the source such that correct push down queries will be sent to the SAS source system (this requires further developement)


To build run
```
mvn clean install -Dteiid.version=8.4.1 -Dsas.version=9.2
```

note you have to provide the SAS jar files in your local maven repository for above to work correctly.


Install the translator in JDV 6 platorm

After sucessfuly running the above build, unzip the "translator-sas/target/translator-sas-1.0-SNAPSHOT-jboss-as7-dist.zip" on the "modules" directory of JDV 6 server to install the translator JAR files. Note that you also need to copy the SAS jar files into the JDV 6. Then edit the standalone.xml file, in the "teiid" subsystem, add the following to add the translator to the configuration

```
<translator name="sas-spds" module="org.jboss.teiid.translator.sas" />
```




# TODO: (tasks to be finished)

1) Add Function support for 

```
 addr
 arsin
 atan
 band
 betainv
 blshift
 bnot
 bor
 brshift
 bxor

 byte
 ceil
 cinv
 collate
 compbl
 compound
 compress
 cos
 cosh
 css
 cv

 daccdb
 daccdbsl
 daccsl
 daccsyd
 dacctab
 date
 datejul
 datepart
 datetime

 day
 dcss
 depdb
 depdbsl
 depsl
 depsyd
 deptab
 dequote
 dhms
 digamma
 dmax

 dmean
 dmin
 drange
 dstd
 dstderr
 dsum
 duss
 dvar
 erf
 erfc
 exp
 finv

 fipname
 fipnamel
 fipstate
 floor
 fnonmiss
 fuzz
 gaminv
 gamma
 hms
 hour

 int
 intck
 intnx
 intrr
 irr
 ispexec
 isplink
 kurtosis
 left
 length
 lgamma

 log
 log10
 log2
 lowcase
 max
 mdy
 mean
 min
 minute
 mod
 month
 mort
 n

 netpv
 nmiss
 npv
 ordinal
 poisson
 probbeta
 probbnml
 probchi
 probf
 probgam

 probhypr
 probit
 probnegb
 probnorm
 probt
 qtr
 quote
 range
 ranuni
 rank

 recip
 repeat
 reverse
 right
 round
 saving
 second
 sign
 signum
 sin
 sinh

 skewness
 sqrt
 std
 stderr
 stfips
 stname
 stnamel
 substr
 sum
 tan
 tanh

 time
 timepart
 tinv
 today
 tranwrd
 trigamma
 trim
 upcase
 uss
 var
 weekday

 year
 zipfips
 zipname
 zipnamel
 zipstate
```
 
 2) The driver seems very sensitive and does not give lot of error information
 
 3) capabilities needs to be carefully vetted
 
 4) testing
