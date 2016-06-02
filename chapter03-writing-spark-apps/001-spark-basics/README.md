# 001-spark-basics

## Application Specs
This program loads a JSON file with the GitHub activity from 2013-05-01 00:00 (as found in `src/main/resources` and performs the count of all the records and the count of all the `PushEvent` records.

## Concepts
Illustrates the very basics of Spark, including the creation of the configuration, the `SparkContext` and `SQLContext` and how to read the contents of a JSON file.
Once the JSON file is loaded, the contents are counted and filtered.

## Notes
It is intended to be used in a local Spark installation.

You can inspect the contents of the files using `head -n 2 src/main/resources/2015-03-01-0.json | jq '.'` which will pretty print the first two lines of one of the downloaded files.
 ```json
 {
  "id": "2614896652",
  "type": "CreateEvent",
  "actor": {
    "id": 739622,
    "login": "treydock",
    "gravatar_id": "",
    "url": "https://api.github.com/users/treydock",
    "avatar_url": "https://avatars.githubusercontent.com/u/739622?"
  },
  "repo": {
    "id": 23934080,
    "name": "Early-Modern-OCR/emop-dashboard",
    "url": "https://api.github.com/repos/Early-Modern-OCR/emop-dashboard"
  },
  "payload": {
    "ref": "development",
    "ref_type": "branch",
    "master_branch": "master",
    "description": "",
    "pusher_type": "user"
  },
  "public": true,
  "created_at": "2015-03-01T00:00:00Z",
  "org": {
    "id": 10965476,
    "login": "Early-Modern-OCR",
    "gravatar_id": "",
    "url": "https://api.github.com/orgs/Early-Modern-OCR",
    "avatar_url": "https://avatars.githubusercontent.com/u/10965476?"
  }
}
{
  "id": "2614896653",
  "type": "PushEvent",
  "actor": {
    "id": 9063348,
    "login": "bezerrathm",
    "gravatar_id": "",
    "url": "https://api.github.com/users/bezerrathm",
    "avatar_url": "https://avatars.githubusercontent.com/u/9063348?"
  },
  "repo": {
    "id": 31481156,
    "name": "bezerrathm/HuffmanCoding",
    "url": "https://api.github.com/repos/bezerrathm/HuffmanCoding"
  },
  "payload": {
    "push_id": 588068425,
    "size": 1,
    "distinct_size": 1,
    "ref": "refs/heads/master",
    "head": "570ad890d78525dfc10364901c41b8236e2c783a",
    "before": "6dda286a3a1c254184f1456b5fefc139ff9dce66",
    "commits": [
      {
        "sha": "570ad890d78525dfc10364901c41b8236e2c783a",
        "author": {
          "email": "bezerrathm@gmail.com",
          "name": "Thiago Henrique Menêses Bezerra"
        },
        "message": "Create other.h",
        "distinct": true,
        "url": "https://api.github.com/repos/bezerrathm/HuffmanCoding/commits/570ad890d78525dfc10364901c41b8236e2c783a"
      }
    ]
  },
  "public": true,
  "created_at": "2015-03-01T00:00:00Z"
}
 ```