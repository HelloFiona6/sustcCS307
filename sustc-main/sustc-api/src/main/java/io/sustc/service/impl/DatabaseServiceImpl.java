package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //replace this with your own student IDs in your group
        return Arrays.asList(12210360, 12210723);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        long start = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
        ) {
            conn.setAutoCommit(false);
            stmt.addBatch("ALTER TABLE follow DROP CONSTRAINT follow_follower_mid_fkey");
            stmt.addBatch("ALTER TABLE follow DROP CONSTRAINT follow_following_mid_fkey");
            stmt.addBatch("ALTER TABLE video DROP CONSTRAINT video_owner_mid_fkey");
            stmt.addBatch("ALTER TABLE thumbs_up DROP CONSTRAINT thumbs_up_user_mid_fkey");
            stmt.addBatch("ALTER TABLE thumbs_up DROP CONSTRAINT thumbs_up_video_bv_fkey");
            stmt.addBatch("ALTER TABLE coin DROP CONSTRAINT coin_user_mid_fkey");
            stmt.addBatch("ALTER TABLE coin DROP CONSTRAINT coin_video_bv_fkey");
            stmt.addBatch("ALTER TABLE favorite DROP CONSTRAINT favorite_user_mid_fkey");
            stmt.addBatch("ALTER TABLE favorite DROP CONSTRAINT favorite_video_bv_fkey");
            stmt.addBatch("ALTER TABLE view DROP CONSTRAINT view_user_mid_fkey");
            stmt.addBatch("ALTER TABLE view DROP CONSTRAINT view_video_bv_fkey");
            stmt.addBatch("ALTER TABLE danmu DROP CONSTRAINT danmu_bv_fkey");
            stmt.addBatch("ALTER TABLE danmu DROP CONSTRAINT danmu_user_mid_fkey");
            stmt.addBatch("ALTER TABLE danmulikeby DROP CONSTRAINT danmulikeby_danmu_id_fkey");
            stmt.addBatch("ALTER TABLE danmulikeby DROP CONSTRAINT danmulikeby_mid_fkey");
            stmt.executeBatch();
            conn.commit();

            Thread thread1 = new Thread(() -> importUsers(userRecords));
            Thread thread2 = new Thread(() -> importFollow(userRecords));
            Thread thread3 = new Thread(() -> importVideo(videoRecords));
            Thread thread4 = new Thread(() -> importView(videoRecords));
            Thread thread5 = new Thread(() -> importCoin(videoRecords));
            Thread thread6 = new Thread(() -> importFavorite(videoRecords));
            Thread thread7 = new Thread(() -> importLikes(videoRecords));
            Thread thread8 = new Thread(() -> importDanmu(danmuRecords));
            Thread thread9 = new Thread(() -> importDanmuLike(danmuRecords));

            thread1.start();
            thread2.start();
            thread3.start();
            thread4.start();
            thread5.start();
            thread6.start();
            thread7.start();
            thread8.start();
            thread9.start();

            try {
                thread1.join();
                thread2.join();
                thread3.join();
                thread4.join();
                thread5.join();
                thread6.join();
                thread7.join();
                thread8.join();
                thread9.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            stmt.addBatch("ALTER TABLE follow ADD CONSTRAINT follow_follower_mid_fkey FOREIGN KEY (follower_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE follow ADD CONSTRAINT follow_following_mid_fkey FOREIGN KEY (following_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE video ADD CONSTRAINT video_owner_mid_fkey FOREIGN KEY (owner_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE thumbs_up ADD CONSTRAINT thumbs_up_video_bv_fkey FOREIGN KEY (video_bv) REFERENCES video(bv)");
            stmt.addBatch("ALTER TABLE thumbs_up ADD CONSTRAINT thumbs_up_user_mid_fkey FOREIGN KEY (user_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE coin ADD CONSTRAINT coin_video_bv_fkey FOREIGN KEY (video_bv) REFERENCES video(bv)");
            stmt.addBatch("ALTER TABLE coin ADD CONSTRAINT coin_user_mid_fkey FOREIGN KEY (user_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE favorite ADD CONSTRAINT favorite_video_bv_fkey FOREIGN KEY (video_bv) REFERENCES video(bv)");
            stmt.addBatch("ALTER TABLE favorite ADD CONSTRAINT favorite_user_mid_fkey FOREIGN KEY (user_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE view ADD CONSTRAINT view_video_bv_fkey FOREIGN KEY (video_bv) REFERENCES video(bv)");
            stmt.addBatch("ALTER TABLE view ADD CONSTRAINT view_user_mid_fkey FOREIGN KEY (user_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE danmu ADD CONSTRAINT danmu_bv_fkey FOREIGN KEY (bv) REFERENCES video(bv)");
            stmt.addBatch("ALTER TABLE danmu ADD CONSTRAINT danmu_user_mid_fkey FOREIGN KEY (user_mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE danmulikeby ADD CONSTRAINT danmulikeby_mid_fkey FOREIGN KEY (mid) REFERENCES users(mid)");
            stmt.addBatch("ALTER TABLE danmulikeby ADD CONSTRAINT danmulikeby_danmu_id_fkey FOREIGN KEY (danmu_id) REFERENCES danmu(danmu_id)");
            stmt.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.println("total: "+ (end-start));
        // print length of every table
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
        System.out.println(danmuRecords.size());
    }
    private void importUsers(List<UserRecord> userRecords) {
        long user_s;
        long user_e;
        long cntUser = 0;
        String userSql = "INSERT INTO users (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try(Connection conn = dataSource.getConnection();
            PreparedStatement userStmt = conn.prepareStatement(userSql);
        ) {
            conn.setAutoCommit(false);
            user_s = System.currentTimeMillis();
            for (UserRecord userRecord : userRecords) {
                userStmt.setLong(1, userRecord.getMid());
                userStmt.setString(2, userRecord.getName());
                userStmt.setString(3, userRecord.getSex());
                userStmt.setString(4, userRecord.getBirthday());
                userStmt.setShort(5, userRecord.getLevel());
                userStmt.setInt(6, userRecord.getCoin());
                userStmt.setString(7, userRecord.getSign());
                userStmt.setString(8, userRecord.getIdentity().name());
                userStmt.setString(9, userRecord.getPassword());
                userStmt.setString(10, userRecord.getQq());
                userStmt.setString(11, userRecord.getWechat());
                userStmt.addBatch();
                cntUser++;
                if (cntUser % 100 == 0) {
                    userStmt.executeBatch();
                }
            }
            userStmt.executeBatch();
            conn.commit();
            user_e = System.currentTimeMillis();
            System.out.println("users: "+(user_e-user_s) );
        }catch (SQLException e){
            throw new RuntimeException(e);
        }

    }
    private void importFollow(List<UserRecord> userRecords){
        long follow_s;
        long follow_e;
        long cntFollow = 0;
        String followSql = "INSERT INTO follow (follower_mid,following_mid) VALUES (?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement followStmt = conn.prepareStatement(followSql)) {
            conn.setAutoCommit(false);
            follow_s = System.currentTimeMillis();

            for (UserRecord userRecord : userRecords) {
                for (long follower : userRecord.getFollowing()) {
                    followStmt.setLong(1, userRecord.getMid());
                    followStmt.setLong(2, follower);
                    followStmt.addBatch();
                    cntFollow++;

                    if (cntFollow % 100 == 0) {
                        followStmt.executeBatch();
                    }
                }
            }
            followStmt.executeBatch();
            conn.commit();
            follow_e = System.currentTimeMillis();
            System.out.println("follow: "+(follow_e-follow_s) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importVideo(List<VideoRecord> videoRecords){
        long video_e;
        long video_s;
        long cntVideo = 0;
        String videoSql = "INSERT INTO video (bv, title, owner_mid, owner_name, commit_time, review_time, public_time, duration, description, reviewer_mid,coin,view,likes,favorite) VALUES (?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement videoStmt = conn.prepareStatement(videoSql)
        ) {
            conn.setAutoCommit(false);
            video_s = System.currentTimeMillis();
            for (VideoRecord videoRecord : videoRecords) {

                videoStmt.setString(1, videoRecord.getBv());
                videoStmt.setString(2, videoRecord.getTitle());
                videoStmt.setLong(3, videoRecord.getOwnerMid());
                videoStmt.setString(4, videoRecord.getOwnerName());
                videoStmt.setTimestamp(5, videoRecord.getCommitTime());
                videoStmt.setTimestamp(6, videoRecord.getReviewTime());
                videoStmt.setTimestamp(7, videoRecord.getPublicTime());
                videoStmt.setFloat(8, videoRecord.getDuration());
                videoStmt.setString(9, videoRecord.getDescription());
                videoStmt.setLong(10, videoRecord.getReviewer());
                videoStmt.setLong(11, videoRecord.getCoin().length);
                videoStmt.setLong(12, videoRecord.getViewerMids().length);
                videoStmt.setLong(13, videoRecord.getLike().length);
                videoStmt.setLong(14, videoRecord.getFavorite().length);
                videoStmt.addBatch();
                cntVideo++;
                if (cntVideo % 100 == 0) {
                    videoStmt.executeBatch();
                }
            }
            videoStmt.executeBatch();
            conn.commit();
            video_e = System.currentTimeMillis();
            System.out.println("video: "+(video_e - video_s) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importFavorite(List<VideoRecord> videoRecords){
        long small_s;
        long small_e;
        long cntFavorite = 0;
        String favoriteSql = "INSERT INTO favorite (video_BV,user_mid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement favoriteStmt = conn.prepareStatement(favoriteSql);
        ) {
            conn.setAutoCommit(false);
            small_s = System.currentTimeMillis();
            for (VideoRecord videoRecord : videoRecords) {
                for (long favorite : videoRecord.getFavorite()) {
                    favoriteStmt.setString(1, videoRecord.getBv());
                    favoriteStmt.setLong(2, favorite);
                    favoriteStmt.addBatch();
                    cntFavorite++;
                    if (cntFavorite % 100 == 0) {
                        favoriteStmt.executeBatch();
                    }
                }
            }
            favoriteStmt.executeBatch();
            conn.commit();
            small_e = System.currentTimeMillis();
            System.out.println("favorite: "+(small_e-small_s) );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importLikes(List<VideoRecord> videoRecords){
        long small_s;
        long small_e;
        long cntLike = 0;
        String likeSql = "INSERT INTO thumbs_up (video_BV,user_mid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement likeStmt = conn.prepareStatement(likeSql);
        ) {
            conn.setAutoCommit(false);
            small_s = System.currentTimeMillis();

            for (VideoRecord videoRecord : videoRecords) {
                for (long like : videoRecord.getLike()) {
                    likeStmt.setString(1, videoRecord.getBv());
                    likeStmt.setLong(2, like);
                    likeStmt.addBatch();
                    cntLike++;
                    if (cntLike % 100 == 0) {
                        likeStmt.executeBatch();
                    }
                }
            }
            likeStmt.executeBatch();
            conn.commit();
            small_e = System.currentTimeMillis();
            System.out.println("likes: "+(small_e-small_s) );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importView(List<VideoRecord> videoRecords){
        long small_s;
        long small_e;
        long cntView = 0;
        String viewSql = "INSERT INTO view (video_BV, user_mid, last_watch_time_duration) VALUES (?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement viewStmt = conn.prepareStatement(viewSql)
        ) {
            conn.setAutoCommit(false);
            small_s = System.currentTimeMillis();

            for (VideoRecord videoRecord : videoRecords) {
                for (int i = 0; i < videoRecord.getViewerMids().length; i++) {
                    viewStmt.setString(1, videoRecord.getBv());
                    viewStmt.setLong(2, videoRecord.getViewerMids()[i]);
                    viewStmt.setFloat(3, videoRecord.getViewTime()[i]);
                    viewStmt.addBatch();
                    cntView++;
                    if (cntView % 100 == 0) {
                        viewStmt.executeBatch();
                    }
                }
            }
            viewStmt.executeBatch();
            conn.commit();
            small_e = System.currentTimeMillis();
            System.out.println("view: "+(small_e-small_s) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importCoin(List<VideoRecord> videoRecords){
        long small_s;
        long small_e;
        long cntCoin = 0;
        String coinSql = "INSERT INTO coin (video_BV,user_mid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement coinStmt = conn.prepareStatement(coinSql);
        ) {
            conn.setAutoCommit(false);
            small_s = System.currentTimeMillis();

            for (VideoRecord videoRecord : videoRecords) {
                for (long coin : videoRecord.getCoin()) {
                    coinStmt.setString(1, videoRecord.getBv());
                    coinStmt.setLong(2, coin);
                    coinStmt.addBatch();
                    cntCoin++;
                    if (cntCoin % 100 == 0) {
                        coinStmt.executeBatch();
                    }
                }
            }
            coinStmt.executeBatch();
            conn.commit();
            small_e = System.currentTimeMillis();
            System.out.println("coin: "+(small_e-small_s) );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importRelationTable(List<VideoRecord> videoRecords){
        long small_s;
        long small_e;
        long cntLike = 0;
        long cntFavorite = 0;
        long cntView = 0;
        long cntCoin = 0;
        String likeSql = "INSERT INTO thumbs_up (video_BV,user_mid) VALUES (?, ?)";
        String coinSql = "INSERT INTO coin (video_BV,user_mid) VALUES (?, ?)";
        String favoriteSql = "INSERT INTO favorite (video_BV,user_mid) VALUES (?, ?)";
        String viewSql = "INSERT INTO view (video_BV, user_mid, last_watch_time_duration) VALUES (?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement likeStmt = conn.prepareStatement(likeSql);
             PreparedStatement coinStmt = conn.prepareStatement(coinSql);
             PreparedStatement favoriteStmt = conn.prepareStatement(favoriteSql);
             PreparedStatement viewStmt = conn.prepareStatement(viewSql)
        ) {
            conn.setAutoCommit(false);
            small_s = System.currentTimeMillis();

            for (VideoRecord videoRecord : videoRecords) {
                for (long like : videoRecord.getLike()) {
                    likeStmt.setString(1, videoRecord.getBv());
                    likeStmt.setLong(2, like);
                    likeStmt.addBatch();
                    cntLike++;
                    if (cntLike % 100 == 0) {
                        likeStmt.executeBatch();
                    }
                }
                for (long coin : videoRecord.getCoin()) {
                    coinStmt.setString(1, videoRecord.getBv());
                    coinStmt.setLong(2, coin);
                    coinStmt.addBatch();
                    cntCoin++;
                    if (cntCoin % 100 == 0) {
                        coinStmt.executeBatch();
                    }
                }
                for (long favorite : videoRecord.getFavorite()) {
                    favoriteStmt.setString(1, videoRecord.getBv());
                    favoriteStmt.setLong(2, favorite);
                    favoriteStmt.addBatch();
                    cntFavorite++;
                    if (cntFavorite % 100 == 0) {
                        favoriteStmt.executeBatch();
                    }
                }
                for (int i = 0; i < videoRecord.getViewerMids().length; i++) {
                    viewStmt.setString(1, videoRecord.getBv());
                    viewStmt.setLong(2, videoRecord.getViewerMids()[i]);
                    viewStmt.setFloat(3, videoRecord.getViewTime()[i]);
                    viewStmt.addBatch();
                    cntView++;
                    if (cntView % 100 == 0) {
                        viewStmt.executeBatch();
                    }
                }
            }
            likeStmt.executeBatch();
            coinStmt.executeBatch();
            favoriteStmt.executeBatch();
            viewStmt.executeBatch();
            conn.commit();
            small_e = System.currentTimeMillis();
            System.out.println("coin,view,favorite,thumbs_up: "+(small_e-small_s) );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importDanmu(List<DanmuRecord> danmuRecords){
        long danmuId = 0;
        long danmu_e;
        long danmu_s;
        long cntDanmu = 0;
        String danmuSql = "INSERT INTO danmu (bv, user_mid, time, content, post_time,danmu_id) VALUES (?, ?, ?, ?, ?, ? )";
        // danmu
        try (Connection conn = dataSource.getConnection();
             PreparedStatement danmuStmt = conn.prepareStatement(danmuSql)) {
            conn.setAutoCommit(false);
            danmu_s = System.currentTimeMillis();

            for (DanmuRecord danmuRecord : danmuRecords) {
                danmuId++;
                danmuStmt.setString(1, danmuRecord.getBv());
                danmuStmt.setLong(2, danmuRecord.getMid());
                danmuStmt.setFloat(3, danmuRecord.getTime());
                danmuStmt.setString(4, danmuRecord.getContent());
                danmuStmt.setTimestamp(5, danmuRecord.getPostTime());
                danmuStmt.setLong(6, danmuId);
                danmuStmt.addBatch();
                cntDanmu++;
                if (cntDanmu % 100 == 0) {
                    danmuStmt.executeBatch();
                }
            }
            danmuStmt.executeBatch();
            conn.commit();
            danmu_e = System.currentTimeMillis();
            System.out.println("danmu: "+(danmu_e-danmu_s) );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    private void importDanmuLike(List<DanmuRecord> danmuRecords){
        long danmuId = 0;
        long danmuId_s;
        long danmuId_e;
        long cntLikeDanmu = 0;
        String DanmuLikeBySql = "INSERT INTO DanmuLikeBy (danmu_id,mid) VALUES (?, ?)";

        // danmu id
        try (Connection conn = dataSource.getConnection();
             PreparedStatement DanmuLikeByStmt = conn.prepareStatement(DanmuLikeBySql)) {
            conn.setAutoCommit(false);
            danmuId_s = System.currentTimeMillis();

            for (DanmuRecord danmuRecord : danmuRecords) {
                danmuId++;
                for (long likeBy : danmuRecord.getLikedBy()) {
                    DanmuLikeByStmt.setLong(1, danmuId);
                    DanmuLikeByStmt.setLong(2, likeBy);
                    DanmuLikeByStmt.addBatch();
                    cntLikeDanmu++;
                    if (cntLikeDanmu % 100 == 0) {
                        DanmuLikeByStmt.executeBatch();
                    }
                }
            }
            DanmuLikeByStmt.executeBatch();
            conn.commit();
            danmuId_e = System.currentTimeMillis();
            System.out.println("danmuLikeBy: "+(danmuId_e-danmuId_s) );

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     * <p>
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */
    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql;

        sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
