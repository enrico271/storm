SSH_KEY=capstone.pem # replace this with the location of your SSH key

# Storm UI will be at http://localhost:9999/index.html, Ganglia will be at http://localhost:9999/ganglia/
ssh -i $SSH_KEY -L 9999:storm00:8080 -L 9998:storm00:80 root@173.39.238.84 -p22228
