PACKAGE=apache-storm-0.9.4.tar.gz
SSH_KEY=~/UCB/CAPSTONE/capstone.pem # replace this with the location of your SSH key

# Compile
cd ..
mvn clean install -DskipTests=true || exit 1

# Package
cd storm-dist/binary/
mvn package || exit 1

# Deploy
scp -i $SSH_KEY -P 22228 target/$PACKAGE root@173.39.238.84:~
ssh -i $SSH_KEY root@173.39.238.84 -p22228 'stormdistribute'
exit