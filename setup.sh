# install environment
python3 -m venv .env
.env/bin/pip install -r requirements.txt
# clone opendetex
git clone https://github.com/pkubowicz/opendetex
(cd opendetex && make) 
cp opendetex/detex ./detex