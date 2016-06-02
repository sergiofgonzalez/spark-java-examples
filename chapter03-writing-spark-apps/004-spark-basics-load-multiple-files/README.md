# 004-spark-basics-load-multiple-files


## Application Specs
Based on the functionality of `003-spark-basics-filter-udf`, this program performs a filter on the results of the previous program matching the results with those found in the `ghEmployees.txt` file. The only difference being that in this case instead of only one file a bunch of files are considered.

## Concepts
The program illustrates how to load a set of files in a `DataFrame`.

## Notes
It is intended to be used in a local Spark installation.

To download the files used in the example you have to run the following commands:

```bash
$ mkdir -p /tmp/github-archive
$ cd /tmp/github-archive
$ wget http://data.githubarchive.org/2015-03-01-{0..23}.json.gz
$ gunzip -v *
```

or use `e01-spring-async-file-downloader` to download the files to your local file system. 

Once downloaded and gunzipped, you can inspect the contents of the files using `head -n 2 /tmp/data/2015-03-01-0.json | jq '.'` which will pretty print the first two lines of one of the downloaded files.
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
