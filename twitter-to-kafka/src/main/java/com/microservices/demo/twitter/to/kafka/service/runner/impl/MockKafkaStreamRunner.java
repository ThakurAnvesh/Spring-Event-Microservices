package com.microservices.demo.twitter.to.kafka.service.runner.impl;

import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Thread.sleep;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
@Slf4j
public class MockKafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private static final Random random = new Random();

    private static final String[] WORDS = new String[] {
            "lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
            "adipiscing", "elit", "curabitur", "vel", "hendrerit", "libero"
    };

    private static final String TWEET_AS_RAW_JSON = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\","+
            "\"text\":\"{2}\","+
            "\"user\":{\"id\":\"{3}\"}"+
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }

    @Override
    public void start() throws TwitterException, InterruptedException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int maxLengthTweet = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        int minLengthTweet = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        long mockSleepMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        infiniteTweets(keywords, maxLengthTweet, minLengthTweet, mockSleepMs);
    }

    private void infiniteTweets(String[] keywords, int maxLengthTweet, int minLengthTweet, long mockSleepMs){
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true){
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minLengthTweet, maxLengthTweet);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    Sleep(mockSleepMs);
                }
            }catch (TwitterException ex){
                log.error("Error creating twitter stream!", ex);
                ex.printStackTrace();
            }
        });
    }

    private void Sleep(long mockSleepMs) throws TwitterException {
        try {
            Thread.sleep(mockSleepMs);
        }catch (InterruptedException ex){
            throw new TwitterException("Error while sleeping");
        }
    }

    private String getFormattedTweet(String[] keywords, int minLengthTweet, int maxLengthTweet) {
        String [] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minLengthTweet, maxLengthTweet),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };

        String tweet = TWEET_AS_RAW_JSON;
        for (int i = 0; i < params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }
        return  tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minLengthTweet, int maxLengthTweet) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = random.nextInt(maxLengthTweet-minLengthTweet + 1) + minLengthTweet;
        for (int i = 0; i < tweetLength; i++) {
            tweet.append(WORDS[random.nextInt(WORDS.length)]).append(" ");
            if(i == tweetLength / 2) {
                tweet.append(keywords[random.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }
}
