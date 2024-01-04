package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.VideoRecord;
import io.sustc.service.UserService;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;
    private static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /**
     * Posts a video. Its commit time shall be {@link LocalDateTime#now()}.
     *
     * @param auth the current user's authentication information
     * @param req  the video's information
     * @return the video's {@code bv}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code req} is invalid
     *     <ul>
     *       <li>{@code title} is null or empty</li>
     *       <li>there is another video with same {@code title} and same user</li>
     *       <li>{@code duration} is less than 10 (so that no chunk can be divided)</li>
     *       <li>{@code publicTime} is earlier than {@link LocalDateTime#now()}</li>
     *     </ul>
     *   </li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        // auth is valid
        if (!isValidAuth(auth)) {
            return null;
        }
        if (!isValidReq(req, auth)) return null;
        String bv = generateBv();

        VideoRecord videoRecord = new VideoRecord(auth.getMid());
        videoRecord.setBv(bv);
        videoRecord.setTitle(req.getTitle());
        videoRecord.setOwnerMid(auth.getMid());
        videoRecord.setOwnerName(getUserName(auth.getMid()));
        videoRecord.setCommitTime(Timestamp.valueOf(LocalDateTime.now()));
        videoRecord.setReviewTime(null);
        videoRecord.setPublicTime(req.getPublicTime());
        videoRecord.setDuration(req.getDuration());
        videoRecord.setDescription(req.getDescription());
        videoRecord.setReviewer(null);

        importVideoRecord(videoRecord);

        return bv;
    }

    private void importVideoRecord(VideoRecord videoRecord) {
        String videoSql = "INSERT INTO video (bv, title, owner_mid, owner_name, commit_time, review_time, public_time, duration, description, reviewer_mid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement videoStmt = conn.prepareStatement(videoSql)) {
                videoStmt.setString(1, videoRecord.getBv());
                videoStmt.setString(2, videoRecord.getTitle());
                videoStmt.setLong(3, videoRecord.getOwnerMid());
                videoStmt.setString(4, videoRecord.getOwnerName());
                videoStmt.setTimestamp(5, videoRecord.getCommitTime());
                videoStmt.setTimestamp(6, videoRecord.getReviewTime());
                videoStmt.setTimestamp(7, videoRecord.getPublicTime());
                videoStmt.setFloat(8, videoRecord.getDuration());
                videoStmt.setString(9, videoRecord.getDescription());
//                videoStmt.setLong(10, videoRecord.getReviewer());
                Long reviewer = videoRecord.getReviewer();
                if (reviewer != null) {
                    videoStmt.setLong(10, reviewer);
                } else {
                    videoStmt.setNull(10, Types.BIGINT);
                }

                videoStmt.execute();
            }
            conn.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * <li>the {@code auth} is invalid
     *     <ul>
     *       <li>both {@code qq} and {@code wechat} are non-empty while they do not correspond to same user</li>
     *       <li>{@code mid} is invalid while {@code qq} and {@code wechat} are both invalid (empty or not found)</li>
     *     </ul>
     *   </li>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    private boolean isValidAuth(AuthInfo auth) {
        boolean validOfMid = false;
        boolean validOfQQ = false;
        boolean validOfWechat = false;
        int numberOfMid = 0;
        int numberOfQQ = 0;
        int numberOfWechat = 0;
        if (auth.getPassword() != null) {
            String sqlOfValidOfMid = "select count(*) as count from users where mid= ? and password = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfValidOfMid)) {
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, auth.getPassword());
                ResultSet resultSet = stmt.executeQuery();
                if (resultSet.next()) {
                    numberOfMid = resultSet.getInt("count");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            validOfMid = numberOfMid == 1;
            if (numberOfMid != 1) return false;
        }
        //QQ is valid
        if (auth.getQq() != null) {
            String sqlOfQQ = "select count(*) as count from users where QQ = ? ";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfQQ)) {
                stmt.setString(1, auth.getQq());
                ResultSet resultSet = stmt.executeQuery();
                if (resultSet.next()) {
                    numberOfQQ = resultSet.getInt("count");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            validOfQQ = numberOfQQ == 1;

            if (validOfQQ) {
                String sqlOfMid = "select mid as mid from users where QQ = ?";
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
                    stmt.setString(1, auth.getQq());
                    ResultSet resultSet = stmt.executeQuery();
                    if (resultSet.next()) {
                        long result = resultSet.getLong("mid");
                        if (auth.getMid() == 0) auth.setMid(result);
                        else {
                            if (auth.getMid() != result) return false;
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        //wechat is valid
        if (auth.getWechat() != null) {
            String sqlOfWechat = "select count(*) as count from users where Wechat = ? ";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfWechat)) {
                stmt.setString(1, auth.getWechat());
                ResultSet resultSet = stmt.executeQuery();
                if (resultSet.next()) {
                    numberOfWechat = resultSet.getInt("count");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            validOfWechat = numberOfWechat == 1;

            if (validOfWechat) {
                String sqlOfMid = "select mid as mid from users where wechat = ?";
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
                    stmt.setString(1, auth.getWechat());
                    ResultSet resultSet = stmt.executeQuery();
                    if (resultSet.next()) {
                        long result = resultSet.getLong("mid");
                        if (auth.getMid() == 0) auth.setMid(result);
                        else {
                            if (auth.getMid() != result) return false;
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return validOfMid || validOfQQ || validOfWechat;
    }

    private boolean isValidReq(PostVideoReq req, AuthInfo auth) {
        if (req.getTitle() == null || req.getTitle().isEmpty()) {
            return false;
        }
        if (req.getDuration() < 10) {
            return false;
        }
        if (req.getPublicTime() == null || req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now()))) {
            return false;
        }
        // have post
        if (isTitleMidDuplicate(req.getTitle(), auth.getMid())) return false;

        return true;
    }

    private String generateBv() {
        String bv;
        do {
            StringBuilder sb = new StringBuilder(10);
            Random random = new Random();
            for (int i = 0; i < 10; i++) {
                int index = random.nextInt(CHARACTERS.length());
                char randomChar = CHARACTERS.charAt(index);
                sb.append(randomChar);
            }
            bv = "BV" + sb;
        } while (isBVDuplicate(bv));
        return bv;
    }

    // true--duplicate
    private boolean isBVDuplicate(String bv) {
        boolean isDuplicate = false;
        String query = "SELECT bv FROM video WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDuplicate = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDuplicate;
    }

    private boolean isTitleDuplicate(String title) {
        boolean isDuplicate = false;
        String query = "SELECT title FROM video WHERE title = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, title);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDuplicate = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDuplicate;
    }

    private boolean isTitleMidDuplicate(String title, long mid) {
        boolean isDuplicate = false;
        String query = "SELECT * FROM video WHERE title = ? and owner_mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, title);
            stmt.setLong(2, mid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDuplicate = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDuplicate;
    }

    private boolean isMidDuplicate(long mid) {
        boolean isDuplicate = false;
        String query = "SELECT * FROM users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDuplicate = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDuplicate;
    }

    private String getUserName(long mid) {
        String query = "SELECT name FROM users WHERE mid = ?";
        String name = null;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                name = resultSet.getString("name");
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return name;
    }

    /**
     * Deletes a video.
     * This operation can be performed by the video owner or a superuser.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return success or not
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code auth} is not the owner of the video nor a superuser</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
//    public boolean deleteVideo(AuthInfo auth, String bv) {
//        // if auth is invalid
//        if(!isValidAuth(auth)) return false;
//        // cannot find video
//        if (!findVideo(bv)) return false;
//        if(!(isMatchMidBv(auth,bv) || isAuthSuperuser(auth))) return false;
//        // delete info except video
//
//        ArrayList<Long> danmuIds = getDanmuId(bv);
//        String deleteDanmuLike = "DELETE FROM DanmuLikeBy WHERE danmu_id = ?";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement danmuLikeStmt = conn.prepareStatement(deleteDanmuLike)) {
//            for (Long danmuId : danmuIds) {
//                danmuLikeStmt.setLong(1, danmuId);
//                danmuLikeStmt.executeUpdate();
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        String deleteVideo = "DELETE FROM video WHERE bv = ?";
//        String deleteView = "delete from view where video_BV= ?";
//        String deleteFavorite = "delete from Favorite where video_BV= ?";
//        String deleteCoin = "delete from coin where video_BV= ?";
//        String deleteLike = "delete from thumbs_up where video_BV= ?";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement viewStmt = conn.prepareStatement(deleteView);
//             PreparedStatement favoriteStmt = conn.prepareStatement(deleteFavorite);
//             PreparedStatement coinStmt = conn.prepareStatement(deleteCoin);
//             PreparedStatement likeStmt = conn.prepareStatement(deleteLike);
//             PreparedStatement videoStmt = conn.prepareStatement(deleteVideo)
//        ) {
//            viewStmt.setString(1, bv);
//            favoriteStmt.setString(1, bv);
//            coinStmt.setString(1, bv);
//            likeStmt.setString(1, bv);
//            videoStmt.setString(1, bv);
//            int rowsAffected = videoStmt.executeUpdate();
//            return rowsAffected > 0;
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    // small ok
    public boolean deleteVideo(AuthInfo auth, String bv) {
        // if auth is invalid
        if (!isValidAuth(auth)) return false;
        // cannot find video
        if (!findVideo(bv)) return false;
        if (!(isMatchMidBv(auth, bv) || isAuthSuperuser(auth))) return false;

        String deleteVideo = "DELETE FROM video WHERE BV = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement videoStmt = conn.prepareStatement(deleteVideo)
        ) {
            videoStmt.setString(1, bv);
            int rowsAffected = videoStmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ArrayList<Long> getDanmuId(String bv) {
        ArrayList<Long> danmuIds = new ArrayList<>();
        String query = "SELECT danmu_id FROM danmu WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                long danmuId = resultSet.getLong("danmu_id");
                danmuIds.add(danmuId);
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return danmuIds;
    }

    private boolean findVideo(String bv) {
        boolean find = false;
        String query = "SELECT * FROM video WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                find = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return find;
    }

    /**
     * Updates the video's information.
     * Only the owner of the video can update the video's information.
     * If the video was reviewed before, a new review for the updated video is required.
     * The duration shall not be modified and therefore the likes, favorites and danmus are not required to update.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @param req  the new video information
     * @return {@code true} if the video needs to be re-reviewed (was reviewed before), {@code false} otherwise
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code auth} is not the owner of the video</li>
     *   <li>{@code req} is invalid, as stated in {@link VideoService#postVideo(AuthInfo, PostVideoReq)}</li>
     *   <li>{@code duration} in {@code req} is changed compared to current one</li>
     *   <li>{@code req} is not changed compared to current information</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    //Wrong answer for [AuthInfo(mid=38606568, password=null, qq=4977628, wechat=null), BV1bt4y1x7Wk, PostVideoReq(title=《 奇 怪 的 小 兔 叽 增 加 了 》488, description=null, duration=397.0, publicTime=2025-12-01 12:00:00.0)]: expected false, got true
    //Wrong answer for [AuthInfo(mid=250858633, password=null, qq=9469689365, wechat=null), BV1cJ411H77j, PostVideoReq(title=《 奇 怪 的 小 兔 叽 增 加 了 》611, description=null, duration=479.0, publicTime=2025-12-01 12:00:00.0)]: expected false, got true
    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        if (!isValidAuth(auth)) return false;
        // find video
        if (!findVideo(bv)) return false;
        // auth is not the owner
        if (!isMatchMidBv(auth, bv)) return false;
        // req is invalid
        if (!isValidReq(req, auth)) return false;
        //duration in req is changed
        if (isDurationChanged(req, bv)) return false;
        // req is not changed
        if (!isReqChanged(req, bv)) return false;
        boolean isRe = isReview(bv);
//        if(bv.equals("BV1bt4y1x7Wk") || bv.equals("BV1cJ411H77j")) System.out.println(bv+isRe);
        // update
        String updateSql = "UPDATE video SET title = ?, description = ? , review_time = null, reviewer_mid = null WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateSql)) {
            stmt.setString(1, req.getTitle());
            stmt.setString(2, req.getDescription());
            stmt.setString(3, bv);
            int rowsAffected = stmt.executeUpdate();
            return rowsAffected > 0 && isRe;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isReview(String bv) {
        boolean isReview = false;
        String string = "";
        String query = "SELECT reviewer_mid FROM video WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                string = resultSet.getString("reviewer_mid");
                if (string == null) return false;
                isReview = true;
            }
            resultSet.close();
//            if(bv.equals("BV1bt4y1x7Wk") || bv.equals("BV1cJ411H77j")) System.out.println(bv+" "+isReview+" "+string);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isReview;
    }

    private boolean isReqChanged(PostVideoReq req, String bv) {
        // title description duration publicTime
        boolean isChange = false;
        String query = "SELECT title, description, public_time FROM video WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                String title = resultSet.getString("title");
                String description = resultSet.getString("description");
                Timestamp publicTime = resultSet.getTimestamp("public_time");
                if (!title.equals(req.getTitle()) || !description.equals(req.getDescription()) || !publicTime.equals(req.getPublicTime())) {
                    isChange = true;
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isChange;
    }

    private boolean isDurationChanged(PostVideoReq req, String bv) {
        boolean isChange = false;
        String query = "SELECT duration FROM video WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                int duration = resultSet.getInt("duration");
                if (duration != req.getDuration()) {
                    isChange = true;
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isChange;
    }

    private boolean isMatchMidBv(AuthInfo auth, String bv) {
        boolean isMatch = false;
        String query = "SELECT * FROM video WHERE owner_mid = ? and BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, auth.getMid());
            stmt.setString(2, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isMatch = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isMatch;
    }

    /**
     * Search the videos by keywords (split by space).
     * You should try to match the keywords case-insensitively in the following fields:
     * <ol>
     *   <li>title</li>
     *   <li>description</li>
     *   <li>owner name</li>
     * </ol>
     * <p>
     * Sort the results by the relevance (sum up the number of keywords matched in the three fields).
     * <ul>
     *   <li>If a keyword occurs multiple times, it should be counted more than once.</li>
     *   <li>
     *     A character in these fields can only be counted once for each keyword
     *     but can be counted for different keywords.
     *   </li>
     *   <li>If two videos have the same relevance, sort them by the number of views.</li>
     * </u
     * <p>
     * Examples:
     * <ol>
     *   <li>
     *     If the title is "1122" and the keywords are "11 12",
     *     then the relevance in the title is 2 (one for "11" and one for "12").
     *   </li>
     *   <li>
     *     If the title is "111" and the keyword is "11",
     *     then the relevance in the title is 1 (one for the occurrence of "11").
     *   </li>
     *   <li>
     *     Consider a video with title "Java Tutorial", description "Basic to Advanced Java", owner name "John Doe".
     *     If the search keywords are "Java Advanced",
     *     then the relevance is 3 (one occurrence in the title and two in the description).
     *   </li>
     * </ol>
     * <p>
     * Unreviewed or unpublished videos are only visible to superusers or the video owner.
     *
     * @param auth     the current user's authentication information
     * @param keywords the keywords to search, e.g. "sustech database final review"
     * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
     * @param pageNum  the page number, starts from 1
     * @return a list of video {@code bv}s
     * @implNote If the requested page is empty, return an empty list
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>{@code keywords} is null or empty</li>
     *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */

//Wrong answer for [AuthInfo(mid=104234979, password=null, qq=140277, wechat=null), %.... .*skjaxjakscnkjasjke@@@@32r2342?.*.*///%%%%%%%%, 10, 1]: expected [], got [BV1rq4y1k7SH]
    /*
Wrong answer for [AuthInfo(mid=423210, password=null, qq=962260, wechat=null), 猫猫     好人  坏 ？ ！, 10, 2]: expected [BV1bt4y1x7Wk, BV1fu411L7FW, BV1P64y1o7RZ, BV1aG4y1z746, BV1Q7411J751, BV1FJ411M7rM, BV1Q3411M79u, BV1ef4y1R71D, BV1Yb4y1q7FH, BV1Kb411L79v], got [BV1P64y1o7RZ, BV1p44y1i7cb, BV1Kb411L79v, BV1sz411z7Qb, BV1aG4y1z746, BV1s7411j7yk, BV1F7411v7MH, BV1PK4y1b7dt, BV1FJ411M7rM, BV1MJ411U7T5]
Wrong answer for [AuthInfo(mid=353312519, password=null, qq=null, wechat=htMh!EWVPruq), you m TUbE mM, 10, 1]: expected [BV1cX4y1L7nq, BV1Yb4y1q7FH, BV1PK4y1b7dt, BV1rq4y1k7SH, BV1FJ411M7rM, BV19t4y1E7hh, BV1Kb411L79v, BV1Mv4y1N7WJ, BV1Q7411J751, BV1bt4y1x7Wk], got [BV1FJ411M7rM, BV19t4y1E7hh, BV1PK4y1b7dt, BV1Yb4y1q7FH, BV1Q7411J751, BV1bt4y1x7Wk, BV1p5411879s, BV1Mv4y1N7WJ, BV1pM4y1e7Ke, BV1ef4y1R71D]
Wrong answer for [AuthInfo(mid=662071, password=null, qq=null, wechat=wxid_E%WvN$sdaJ@eZBij), AbcDefGHIjKL 你们 UP !! 这 啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊, 3, 2]: expected [BV1rq4y1k7SH, BV1Q7411J751, BV1ef4y1R71D], got [BV1rq4y1k7SH, BV1mG4y1z761, BV1bt4y1x7Wk]
Wrong answer for [AuthInfo(mid=1930343, password=null, qq=673500633, wechat=null), 谁 我的 官方, 10, 1]: expected [BV1PK4y1b7dt, BV1Rg411w7uU, BV1rq4y1k7SH, BV1FJ411M7rM, BV1pM4y1e7Ke, BV1JZ4y1h7nD, BV187411H782, BV1s7411j7yk], got [BV1PK4y1b7dt, BV1rq4y1k7SH, BV1Rg411w7uU, BV1FJ411M7rM, BV1pM4y1e7Ke, BV1JZ4y1h7nD, BV187411H782, BV1s7411j7yk]
Wrong answer for [AuthInfo(mid=270397, password=null, qq=7091374, wechat=null), . \d+, 10, 1]: expected [BV1p5411879s, BV1Kb411L79v, BV1rq4y1k7SH, BV19t4y1E7hh, BV1U94y1d71R, BV1Mv4y1N7WJ, BV1FJ411M7rM, BV1aU4y1G7ek, BV1aG4y1z746, BV15u41187N1], got [BV19t4y1E7hh, BV1FJ411M7rM, BV1p5411879s, BV1Mv4y1N7WJ, BV1U94y1d71R, BV1aU4y1G7ek, BV1Kb411L79v, BV1rq4y1k7SH, BV1aG4y1z746, BV15u41187N1]
Wrong answer for [AuthInfo(mid=494462, password=znlV=^hI, qq=null, wechat=null), 狗 为什么  这个 好的！ ？？？？？ ！@ 哦 xiexienimen 你 们 幸 苦 了 感谢 co p i lo t在 数据 生成时 作出的 巨大 贡献, 17, 3]: expected [BV1XP4y1T7zz, BV1p44y1i7cb], got [BV1p44y1i7cb, BV1aG4y1z746, BV1Q3411M79u, BV1Wh4y1G74h, BV1U94y1d71R, BV1aU4y1G7ek, BV1j5411H7zk, BV1Ah411m7GM, BV1vZ4y187Zs, BV15u41187N1]

     */
    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if (!isValidAuth(auth)) return null;
        if (keywords == null || keywords.isEmpty()) return null;
        if (pageSize <= 0 || pageNum <= 0) return null;

//        String[] keywordArray = keywords.split(" ");
        String[] keywordArray = keywords.split("\\s+");
//        System.out.println(Arrays.toString(keywordArray));
        for (int i = 0; i < keywordArray.length; i++) {
            keywordArray[i] = Pattern.quote(keywordArray[i]).replaceAll("\\\\", "\\\\\\\\").replace("?", "\\?");
//            keywordArray[i] = keywordArray[i].replace("\\\\", "\\\\\\\\");
//            keywordArray[i] = keywordArray[i].replace("?", "\\?");
//            keywordArray[i] = Pattern.quote(keywordArray[i]).replace("?", "\\?");
        }
//        System.out.println(Arrays.toString(keywordArray));
        StringBuilder stringBuilder = new StringBuilder();
//        stringBuilder.append("[");

        for (int i = 0; i < keywordArray.length; i++) {
            stringBuilder.append("'");
            stringBuilder.append(keywordArray[i]);
            stringBuilder.append("'");

            if (i != keywordArray.length - 1) {
                stringBuilder.append(",");
            }
        }
//        stringBuilder.append("]");

//        System.out.println(String.valueOf(stringBuilder));

        List<String> search = new ArrayList<>();
        String bv;
        String sql1 = "SELECT * FROM (SELECT DISTINCT bv,\n" +
                "                      SUM(array_length(regexp_split_to_array(lower(title || description || owner_name), keyword), 1) - 1)OVER (PARTITION BY bv) AS count,\n" +
                "                      view\n" +
                "      FROM video CROSS JOIN unnest(ARRAY [?]) AS keyword\n" +
                "      WHERE lower(title || description || owner_name) ~* keyword ";
        String sql2 = " ORDER BY count DESC, view DESC) AS subquery OFFSET ? LIMIT ?;";
        String mid = " AND review_time IS NOT NULL AND public_time <= current_timestamp ";
        String sql;
        if (isAuthSuperuser(auth)) sql = sql1 + sql2;
        else sql = sql1 + mid + sql2;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
//            stmt.setString(1, String.valueOf(stringBuilder));
            Array array = conn.createArrayOf("varchar", keywordArray);
            stmt.setArray(1,array);
            stmt.setInt(2, (pageNum-1) * pageSize);
            stmt.setInt(3, pageSize);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                bv = resultSet.getString(1);
//                if(auth.getMid()==1660139 || auth.getMid()== 1930343) System.out.println(bv);
                search.add(bv);
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return search;

//        // build query
//        StringBuilder sqlBuilder = new StringBuilder();
//        sqlBuilder.append("SELECT bv FROM video WHERE ");
//        for (int i = 0; i < keywordArray.length; i++) {
//            if (i > 0) {
//                sqlBuilder.append(" OR ");
//            }
//            sqlBuilder.append("LOWER(title) LIKE ?");
//            sqlBuilder.append(" OR LOWER(description) LIKE ?");
//            sqlBuilder.append(" OR LOWER(owner_name) LIKE ?");
//        }
//        sqlBuilder.append(" ORDER BY (");
//        for (int i = 0; i < keywordArray.length; i++) {
//            if (i > 0) {
//                sqlBuilder.append(" + ");
//            }
//            sqlBuilder.append("CASE WHEN LOWER(title) LIKE ? THEN 1 ELSE 0 END");
//            sqlBuilder.append(" + CASE WHEN LOWER(description) LIKE ? THEN 1 ELSE 0 END");
//            sqlBuilder.append(" + CASE WHEN LOWER(owner_name) LIKE ? THEN 1 ELSE 0 END");
//        }
//        sqlBuilder.append(") DESC, view DESC");
//        sqlBuilder.append(" LIMIT ? OFFSET ?");
//
//        String searchSql = sqlBuilder.toString();
//
//        // connect to database
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(searchSql)) {
//            int paramIndex = 1;
//            for (String keyword : keywordArray) {
//                String lowerKeyword = keyword.toLowerCase();
//                stmt.setString(paramIndex++, "%" + lowerKeyword + "%"); // title
//                stmt.setString(paramIndex++, "%" + lowerKeyword + "%"); // description
//                stmt.setString(paramIndex++, "%" + lowerKeyword + "%"); // owner_mid
//            }
//            for (String keyword : keywordArray) {
//                String lowerKeyword = keyword.toLowerCase();
//                stmt.setString(paramIndex++, "%" + lowerKeyword + "%");
//                stmt.setString(paramIndex++, "%" + lowerKeyword + "%");
//                stmt.setString(paramIndex++, "%" + lowerKeyword + "%");
//            }
//            stmt.setInt(paramIndex++, pageSize);
//            stmt.setInt(paramIndex, (pageNum - 1) * pageSize);
//
//            List<String> result = new ArrayList<>();
//            try (ResultSet rs = stmt.executeQuery()) {
//                while (rs.next()) {
//                    String bv = rs.getString("bv");
//                    // Unreviewed or unpublished videos are only visible to superusers or the video owner
//                    if (isVideoReviewed(bv) || isAuthSuperuser(auth) || isMatchMidBv(auth, bv)) result.add(bv);
//                }
//            }
//            return result;
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
    }

    private boolean isAuthSuperuser(AuthInfo auth) {
        boolean is = false;
        String query = "select identity from users where mid=?";
        String identity = null;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, auth.getMid());
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                identity = resultSet.getString("identity");
                if (identity.equals("SUPERUSER")) is = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return is;
    }

    private boolean isVideoReviewed(String bv) {
        boolean isReviewed = false;
        String query = "select reviewer_mid from video where bv =?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isReviewed = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isReviewed;
    }

    /**
     * Calculates the average view rate of a video.
     * The view rate is defined as the user's view time divided by the video's duration.
     *
     * @param bv the video's {@code bv}
     * @return the average view rate
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>no one has watched this video</li>
     * </ul>
     * If any of the corner case happened, {@code -1} shall be returned.
     */
    // ok
    @Override
    public double getAverageViewRate(String bv) {
        if (!findVideo(bv)) return -1;
        if (!isVideoWatched(bv)) return -1;
        // find rate
        String query = "select average/v.duration as ave from video v join (select avg(last_watch_time_duration) as average, video_BV from View where video_BV=? group by video_BV) x on v.BV = x.video_BV";
        double avg = -1;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                avg = resultSet.getDouble("ave");
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return avg;
    }

    private boolean isVideoWatched(String bv) {
        boolean isWatched = false;
        String query = "SELECT * FROM view WHERE video_BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isWatched = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isWatched;
    }

    /**
     * Gets the hotspot of a video.
     * With splitting the video into 10-second chunks, hotspots are defined as chunks with the most danmus.
     *
     * @param bv the video's {@code bv}
     * @return the index of hotspot chunks (start from 0)
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>no one has sent danmu on this video</li>
     * </ul>
     * If any of the corner case happened, an empty set shall be returned.
     */
    @Override
    public Set<Integer> getHotspot(String bv) {
        if (!findVideo(bv)) return Collections.emptySet();
        // no one send danmu
        if (!isDanmu(bv)) return Collections.emptySet();
        Set<Integer> hotspotChunks = new HashSet<>();
//        String query = "SELECT floor(time / 10)  AS chunk FROM danmu WHERE BV = ? GROUP BY chunk ORDER BY COUNT(*) DESC LIMIT 1";
        String query = "SELECT chunk FROM (\n" +
                "    SELECT floor(time / 10) AS chunk, COUNT(*) AS count\n" +
                "    FROM danmu\n" +
                "    WHERE BV = ?\n" +
                "    GROUP BY chunk\n" +
                "    ORDER BY count DESC\n" +
                ") AS subquery\n" +
                "WHERE count = (SELECT MAX(count) FROM (\n" +
                "    SELECT floor(time / 10) AS chunk, COUNT(*) AS count\n" +
                "    FROM danmu\n" +
                "    WHERE BV = ?\n" +
                "    GROUP BY chunk\n" +
                ") AS max_subquery);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            stmt.setString(2, bv);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                int chunk = resultSet.getInt("chunk");
                hotspotChunks.add(chunk);
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return hotspotChunks;
    }

    private boolean isDanmu(String bv) {
        boolean isDanmu = false;
        String query = "SELECT * FROM danmu WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isDanmu = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isDanmu;
    }

    /**
     * Reviews a video by a superuser.
     * If the video is already reviewed, do not modify the review info.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return {@code true} if the video is newly successfully reviewed, {@code false} otherwise
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>{@code auth} is not a superuser or he/she is the owner</li>
     *   <li>the video is already reviewed</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {
        if (!isValidAuth(auth)) return false;
        if (!findVideo(bv)) return false;
        if (!isAuthSuperuser(auth)) return false;
        if (isVideoReviewed(bv)) return false;

        String updateVideo = "UPDATE video SET reviewer_mid = ?, review_time = ? WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateVideo)) {

            stmt.setLong(1, auth.getMid());
            stmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            stmt.setString(3, bv);

            int rowsAffected = stmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Donates one coin to the video. A user can at most donate one coin to a video.
     * The user can only coin a video if he/she can search it ({@link VideoService#searchVideo(AuthInfo, String, int, int)}).
     * It is not mandatory that the user shall watch the video first before he/she donates coin to it.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return whether a coin is successfully donated
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>the user cannot search this video or he/she is the owner</li>
     *   <li>the user has no coin or has donated a coin to this video</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    // todo find mid
    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        if (!isValidAuth(auth)) return false;
        if (!findVideo(bv)) return false;
        if (!isVideoReviewed(bv) && !isAuthSuperuser(auth)) return false;
        if (isMatchMidBv(auth, bv)) return false;
        if (getAuthCoin(auth) == 0) return false;
        if (isCoinVideo(auth.getMid(), bv)) return false;
        // update the number of coin in the coin table
        // Decrease user's coin count by 1
        String updateCoin = "UPDATE Users SET coin = coin - 1 WHERE mid = ?";
        String updateVideo = "UPDATE video SET coin = coin + 1 WHERE bv = ?";
        String insertCoin = "INSERT INTO coin (video_BV, user_mid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // Update the number of coin in the coin table
            // Decrease user's coin count by 1
            try (PreparedStatement stmt = conn.prepareStatement(updateCoin)) {
                stmt.setLong(1, auth.getMid());
                stmt.executeUpdate();
            }

            try (PreparedStatement stmt = conn.prepareStatement(updateVideo)) {
                stmt.setString(1, bv);
                stmt.executeUpdate();
            }
            // Insert coin record
            try (PreparedStatement stmt = conn.prepareStatement(insertCoin)) {
                stmt.setString(1, bv);
                stmt.setLong(2, auth.getMid());
                int rowsAffected = stmt.executeUpdate();
                conn.commit();
                return rowsAffected > 0;
            } catch (SQLException e) {
                conn.rollback();
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean isCoinVideo(Long mid, String bv) {
        boolean isMatch = false;
        String query = "SELECT * FROM coin WHERE user_mid = ? and video_bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, mid);
            stmt.setString(2, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isMatch = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isMatch;
    }

    private int getAuthCoin(AuthInfo auth) {
        String selectCoin = "SELECT coin FROM Users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(selectCoin)) {

            stmt.setLong(1, auth.getMid());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("coin");
                } else {
                    return 0;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Likes a video.
     * The user can only like a video if he/she can search it ({@link VideoService#searchVideo(AuthInfo, String, int, int)}).
     * If the user already liked the video, the operation will cancel the like.
     * It is not mandatory that the user shall watch the video first before he/she likes to it.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return the like state of the user to this video after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>the user cannot search this video or the user is the video owner</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    /*
Wrong answer for [AuthInfo(mid=875505, password=null, qq=214328647, wechat=null), BV1ef4y1R71D]: expected false, got true
Wrong answer for [AuthInfo(mid=217304, password=kCcdfog(oo, qq=null, wechat=null), BV1mG4y1z761]: expected false, got true
Wrong answer for [AuthInfo(mid=1804386, password=%OTlEZMEvK+FN*, qq=null, wechat=null), BV1pM4y1e7Ke]: expected false, got true
     */
    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        if (!isValidAuth(auth)) return false;
        if (!findVideo(bv)) return false;
        if (!isVideoReviewed(bv) && !isAuthSuperuser(auth)) return false;
        if (isMatchMidBv(auth, bv)) return false;
        if (isLikedVideo(auth.getMid(), bv)) {
            String updateVideo = "UPDATE video SET likes = likes - 1 WHERE bv = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(updateVideo)) {
                stmt.setString(1, bv);
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            String deleteLikes = "delete from thumbs_up where user_mid=? and video_bv=?\n";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(deleteLikes)) {
                stmt.setLong(1, auth.getMid());
                stmt.setString(2, bv);
                stmt.executeUpdate();
                return false;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        String updateVideo = "UPDATE video SET likes = likes + 1 WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateVideo)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // update the like in the thumbs_up table
        String insertLikes = "insert into thumbs_up (video_BV, user_mid) values (?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(insertLikes)) {

            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());

            int rowsAffected = stmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean isLikedVideo(Long mid, String bv) {
        boolean isMatch = false;
        String query = "SELECT * FROM thumbs_up WHERE user_mid = ? and video_bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, mid);
            stmt.setString(2, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isMatch = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isMatch;
    }

    /**
     * Collects a video.
     * The user can only collect a video if he/she can search it.
     * If the user already collected the video, the operation will cancel the collection.
     * It is not mandatory that the user shall watch the video first before he/she collects coin to it.
     *
     * @param auth the current user's authentication information
     * @param bv   the video's {@code bv}
     * @return the collect state of the user to this video after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>the user cannot search this video or the user is the video owner</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        if (!isValidAuth(auth)) return false;
        if (!findVideo(bv)) return false;
        if (!isVideoReviewed(bv) && !isAuthSuperuser(auth)) return false;
        if (isMatchMidBv(auth, bv)) return false;
        if (isFavoriteVideo(auth.getMid(), bv)) return false;


        String updateVideo = "UPDATE video SET favorite = favorite + 1 WHERE bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateVideo)) {
            stmt.setString(1, bv);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        // update the like in the thumbs_up table
        String insertCoin = "insert into favorite (video_BV, user_mid) values (?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(insertCoin)) {

            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());

            int rowsAffected = stmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isFavoriteVideo(long mid, String bv) {
        boolean isMatch = false;
        String query = "SELECT * FROM favorite WHERE user_mid = ? and video_bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setLong(1, mid);
            stmt.setString(2, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                isMatch = true;
            }
            resultSet.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return isMatch;
    }
}
