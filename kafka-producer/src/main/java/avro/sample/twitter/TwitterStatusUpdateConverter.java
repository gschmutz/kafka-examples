package avro.sample.twitter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter.sample.domain.Tweet;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.UserMentionEntity;
import ch.trivadis.sample.twitter.avro.v1.TwitterHashtagEntity;
import ch.trivadis.sample.twitter.avro.v1.TwitterStatusUpdate;
import ch.trivadis.sample.twitter.avro.v1.TwitterURLEntity;
import ch.trivadis.sample.twitter.avro.v1.TwitterUser;
import ch.trivadis.sample.twitter.avro.v1.TwitterUserMentionEntity;

public class TwitterStatusUpdateConverter {

	private static Logger logger = LoggerFactory
			.getLogger(TwitterStatusUpdateConverter.class);

	private static TwitterStatusUpdate initializeTwitterStatusUpdate() {
		TwitterStatusUpdate TwitterStatusUpdate = new TwitterStatusUpdate();
		TwitterStatusUpdate.setUser(new TwitterUser());

		return TwitterStatusUpdate;
	}

	public static TwitterStatusUpdate convert(Status status) {
		System.out.println(status.getText());
		TwitterStatusUpdate TwitterStatusUpdate = initializeTwitterStatusUpdate();
		try {
			/* Setting contributors in TwitterStatusUpdate */
			ArrayList<Long> contributors = new ArrayList<Long>();
			for (long contributor : status.getContributors())
				contributors.add(contributor);

			/* Setting basic elements in TwitterStatusUpdate */
			TwitterStatusUpdate.setCreatedAt(DateUtil.getDateAsString(status.getCreatedAt()));
			TwitterStatusUpdate.setCreatedAtAsLong(status.getCreatedAt().getTime());
			TwitterStatusUpdate.setTweetId(status.getId());
			TwitterStatusUpdate.setText(status.getText());
			TwitterStatusUpdate.setIsRetweet(status.isRetweet());

			/* Setting basic GeoLocation elements */
			if (status.getGeoLocation() != null) {
				TwitterStatusUpdate.setCoordinatesLatitude(status
						.getGeoLocation().getLatitude());
				TwitterStatusUpdate.setCoordinatesLongitude(status
						.getGeoLocation().getLongitude());
			}

			/* Setting TwitterHashtagEntities */
			ArrayList<TwitterHashtagEntity> hashtagEntities = new ArrayList<TwitterHashtagEntity>();
			for (HashtagEntity hashtagEntity : status.getHashtagEntities()) {
				TwitterHashtagEntity simpleHashtagEntity = new TwitterHashtagEntity();
				simpleHashtagEntity.setEnd(hashtagEntity.getEnd());
				simpleHashtagEntity.setStart(hashtagEntity.getStart());
				simpleHashtagEntity.setText(hashtagEntity.getText());
				hashtagEntities.add(simpleHashtagEntity);
			}
			TwitterStatusUpdate.setHashtagEntities(hashtagEntities);

			/* Setting SimpleURLEntities */
			ArrayList<TwitterURLEntity> urlEntities = new ArrayList<TwitterURLEntity>();
			for (URLEntity urlEntity : status.getURLEntities()) {
				TwitterURLEntity simpleURLEntity = new TwitterURLEntity();
				simpleURLEntity.setEnd(urlEntity.getEnd());
				simpleURLEntity.setStart(urlEntity.getStart());
				simpleURLEntity.setURL(urlEntity.getURL());
				simpleURLEntity.setDisplayURL(urlEntity.getDisplayURL());
				simpleURLEntity.setExpandedURL(urlEntity.getExpandedURL());
				urlEntities.add(simpleURLEntity);
			}
			TwitterStatusUpdate.setUrlEntities(urlEntities);

			/* Setting SimpleUserMentionEntities */
			ArrayList<TwitterUserMentionEntity> userMentionEntities = new ArrayList<TwitterUserMentionEntity>();
			for (UserMentionEntity userMentionEntity : status
					.getUserMentionEntities()) {
				TwitterUserMentionEntity simpleUserMentionEntity = new TwitterUserMentionEntity();
				simpleUserMentionEntity.setEnd(userMentionEntity.getEnd());
				simpleUserMentionEntity.setStart(userMentionEntity.getStart());
				simpleUserMentionEntity.setId(userMentionEntity.getId());
				simpleUserMentionEntity.setName(userMentionEntity.getName());
				simpleUserMentionEntity.setScreenName(userMentionEntity
						.getScreenName());
				userMentionEntities.add(simpleUserMentionEntity);
			}
			TwitterStatusUpdate.setUserMentionEntities(userMentionEntities);

			/* Setting SimpleUser */
			TwitterUser simpleUser = new TwitterUser();
			simpleUser.setId(status.getUser().getId());
			simpleUser.setScreenName(status.getUser().getScreenName());
			simpleUser.setFollowersCount(status.getUser().getFollowersCount());
			simpleUser.setFriendsCount(status.getUser().getFriendsCount());
			simpleUser.setProfileImageURL(status.getUser().getProfileImageURL());
			TwitterStatusUpdate.setUser(simpleUser);

		} catch (Exception exception) {
			logger.error(
					"Exception while converting a Status object to a TwitterStatusUpdate object",
					exception);
			// crisisMailer.sendEmailAlert(exception);
		}

		return TwitterStatusUpdate;
	}
	
	public Tweet convert(TwitterStatusUpdate status) {
		
		List<String> hashtags = new ArrayList<String>();
		for (TwitterHashtagEntity entity : status.getHashtagEntities()) {
			hashtags.add(entity.getText().toString());
		}
		List<String> urls = new ArrayList<String>();
		List<String> expandedUrls = new ArrayList<String>();
		for (TwitterURLEntity entity : status.getUrlEntities()) {
			urls.add(entity.getURL().toString());
			expandedUrls.add(entity.getExpandedURL().toString());
		}

		Double latitude = status.getCoordinatesLatitude();
		Double longitude = status.getCoordinatesLongitude();
		Date createdAt = DateUtil.toDate(status.getCreatedAt().toString());

		Tweet tweet = new Tweet(status.getTweetId(), createdAt, status.getUser().getScreenName().toString(), status.getText().toString(), latitude, longitude, hashtags);
		
		return tweet;
	}	
	
}
