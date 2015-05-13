SSH_KEY=~/UCB/CAPSTONE/capstone.pem # replace this with the location of your SSH key

ssh -i $SSH_KEY -L 9999:storm00:8080 -L 9998:storm00:80 root@173.39.238.84 -p22228