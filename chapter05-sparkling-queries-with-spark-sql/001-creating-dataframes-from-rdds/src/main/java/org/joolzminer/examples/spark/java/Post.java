package org.joolzminer.examples.spark.java;

import java.io.Serializable;
import java.sql.Timestamp;
import static org.joolzminer.examples.spark.java.utils.SafeConversions.*;

@SuppressWarnings("serial")
public class Post implements Serializable {
	Integer commentCount;
	Timestamp lastActivityDate;
	String ownerUserId;
	String body;
	Integer score;
	Timestamp creationDate;
	Integer viewCount;
	String title;
	String tags;
	Integer answerCount;
	Long acceptedAnswerId;
	Long postTypeId;
	Long id;
	
	public Post(String[] postFields) {
		this.commentCount = toSafeInteger(postFields[0]);
		this.lastActivityDate = toSafeTimestamp(postFields[1]);
		this.ownerUserId = postFields[2];
		this.body = postFields[3];
		this.score = toSafeInteger(postFields[4]);
		this.creationDate = toSafeTimestamp(postFields[5]);
		this.viewCount = toSafeInteger(postFields[6]);
		this.title = postFields[7];
		this.tags = postFields[8];
		this.answerCount = toSafeInteger(postFields[9]);
		this.acceptedAnswerId = toSafeLong(postFields[10]);
		this.postTypeId = toSafeLong(postFields[11]);
		this.id = toSafeLong(postFields[12]);
	}

	public Integer getCommentCount() {
		return commentCount;
	}

	public Timestamp getLastActivityDate() {
		return lastActivityDate;
	}

	public String getOwnerUserId() {
		return ownerUserId;
	}

	public String getBody() {
		return body;
	}

	public Integer getScore() {
		return score;
	}

	public Timestamp getCreationDate() {
		return creationDate;
	}

	public Integer getViewCount() {
		return viewCount;
	}

	public String getTitle() {
		return title;
	}

	public String getTags() {
		return tags;
	}

	public Integer getAnswerCount() {
		return answerCount;
	}

	public Long getAcceptedAnswerId() {
		return acceptedAnswerId;
	}

	public Long getPostTypeId() {
		return postTypeId;
	}

	public Long getId() {
		return id;
	}

	@Override
	public String toString() {
		return "Post [commentCount=" + commentCount + ", lastActivityDate=" + lastActivityDate + ", ownerUserId="
				+ ownerUserId + ", body=" + body + ", score=" + score + ", creationDate=" + creationDate
				+ ", viewCount=" + viewCount + ", title=" + title + ", tags=" + tags + ", answerCount=" + answerCount
				+ ", acceptedAnswerId=" + acceptedAnswerId + ", postTypeId=" + postTypeId + ", id=" + id + "]";
	}
	
	
}
