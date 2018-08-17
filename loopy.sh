while true
do
	GOOGLE_DS_KEY_FILENAME=./test.json GOOGLE_DS_PROJECT_ID=test-project-id npm test
	if [ $? != 0 ]; then
        break
    fi
done