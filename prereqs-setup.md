- ***Anaconda***
```bash 
wget https://repo.anaconda.com/archive/Anaconda3-2021.11-Linux-x86_64.sh #Check for newer release
bash Anaconda3-2021.11-Linux-x86_64.sh #Press "yes" to everything
source .bashrc #To run up Anaconda base
```
- ***Docker + Docker-compose***
```bash
sudo apt-get install docker.io # Start with Docker installation

sudo groupadd docker
sudo gpasswd -a $USER docker
sudo service docker restart #Relogin after last command
```
```bash
mkdir bin/ # To collect binaries (executable apps)
cd bin
wget https://github.com/docker/compose/releases/download/v2.2.3/docker-compose-linux-x86_64 -O docker-compose
chmod +x docker-compose

nano .bashrc

# Add this stroke to the bottom of .bashrc and save it: $ export PATH="${HOME}/bin:${PATH}"

source .bashrc
```
