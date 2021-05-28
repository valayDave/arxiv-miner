sudo apt-get update

sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl

sudo apt-get install -y make flex texlive-full 

git clone https://github.com/pkubowicz/opendetex
(cd opendetex && make) 
cp opendetex/detex ./detex