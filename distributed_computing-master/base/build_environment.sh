wget https://repo.anaconda.com/archive/Anaconda3-2020.11-Linux-x86_64.sh
bash Anaconda3-2020.11-Linux-x86_64.sh -b -p ~/anaconda
rm -rf Anaconda3-*
echo 'export PATH="~/anaconda/bin:$PATH"' >> ~/.bashrc