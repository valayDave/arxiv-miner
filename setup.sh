sudo apt-get update

sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl

curl https://pyenv.run | bash
pyenv install 3.7.2
pyenv global 3.7.2

sudo apt-get install -y make
sudo apt-get install flex

# install environment
sudo apt-get install texlive-full -y
python -m venv .env
# python3 -m venv .env
.env/bin/pip install -r requirements.txt
# clone opendetex
git clone https://github.com/pkubowicz/opendetex
(cd opendetex && make) 
cp opendetex/detex ./detex