pip install -r nltk cso-classifier
python - << EOF
import classifier.classifier as classifier
classifier.setup()
exit() # it is important to close the current console, to make those changes effective
EOF