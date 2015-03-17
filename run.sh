#**********************
#*
#* Progam Name: MP1. Membership Protocol.
#*
#* Current file: run.sh
#* About this file: Submission shell script.
#* 
#***********************
#!/bin/sh
mkdir grade-dir
cd grade-dir
sudo wget https://spark-public.s3.amazonaws.com/cloudcomputing/assignments/mp1/mp1.zip || { echo 'ERROR ... Please install wget'; exit 1; }
sudo unzip -j mp1.zip || { echo 'ERROR ... Zip file not found' ; exit 1; }
sudo cp ../MP1Node.* .
make clean > /dev/null
make > /dev/null
case $1 in
	0) echo "Single failure"
	./Application singlefailure.conf > /dev/null;;
	1) echo "Multiple failure"
	./Application multifailure.conf > /dev/null;;
	2) echo "Single failure with Messages Drop"
	./Application msgdropsinglefailure.conf > /dev/null;;
	*) echo "Please enter a valid option";;
esac
cp dbg.log ../
cd ..
sudo rm -rf grade-dir
