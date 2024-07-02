package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;

    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (!validAuth(auth)) return -1;

        if (!existBv(bv)) return -1;

        if (content == null || content.isEmpty()) return -1;

        String sqlOfTime = "select duration from video where bv = ? ";
        double length = 0.0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfTime)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                length = resultSet.getDouble("duration");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (time > length) return -1;

        if (!bvPublicTime(bv)) return -1;

        String sqlOfWatch = "select count(*) as cnt from view where video_bv = ? and user_mid = ?";
        int numberOfWatch = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfWatch)) {
            stmt.setString(1, bv);
            stmt.setLong(2, auth.getMid());
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfWatch = resultSet.getInt("cnt");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (numberOfWatch != 1) return -1;


        String sqlOfMaxMid = "select max(danmu_id) as max from danmu";
        long MaxID = 0L;
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sqlOfMaxMid)) {
            while (resultSet.next()) {
                MaxID = resultSet.getInt("max");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return MaxID + 1;
    }

    /**
     * Display the danmus in a time range.
     * Similar to bilibili's mechanism, user can choose to only display part of the danmus to have a better watching
     * experience.
     *
     * @param bv        the video's bv
     * @param timeStart the start time of the range
     * @param timeEnd   the end time of the range
     * @param filter    whether to remove the duplicated content,
     *                  if {@code true}, only the earliest posted danmu with the same content shall be returned
     * @return a list of danmus id, sorted by {@code time}
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     *   <li>
     *     {@code timeStart} and/or {@code timeEnd} is invalid ({@code timeStart} <= {@code timeEnd}
     *     or any of them < 0 or > video duration)
     *   </li>
     * <li>the video is not published</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */
    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if (!existBv(bv)) return null;

        String sqlOfDuration = "select duration from video where bv = ? ";
        float duration = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDuration)) {
            stmt.setString(1, bv);

            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                duration = resultSet.getFloat("duration");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (timeEnd <= timeStart || timeEnd > duration || timeEnd < 0 || timeStart < 0 || timeStart > duration) {
            return null;
        }

        if (!bvPublicTime(bv)) return null;

        //List
        List<Long> list = new ArrayList<>();

        String sqlOfWatch;
        if (filter) {
            sqlOfWatch = "select danmu_id as id from danmu where time between ? and ? order by post_time";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfWatch)) {
                stmt.setFloat(1, timeStart);
                stmt.setFloat(2, timeEnd);
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    list.add(resultSet.getLong("id"));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } else {
            sqlOfWatch = """
                    SELECT danmu_id AS id
                    FROM (
                        SELECT MIN(post_time) OVER (PARTITION BY content) AS min_time, danmu_id
                        FROM (
                            SELECT *
                            FROM danmu
                            WHERE BV = ? AND time BETWEEN ? AND ?
                            ORDER BY post_time
                        ) a
                        ORDER BY min_time
                    ) b
                    """;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfWatch)) {
                stmt.setString(1, bv);
                stmt.setFloat(2, timeStart);
                stmt.setFloat(3, timeEnd);
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    list.add(resultSet.getLong("id"));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return list;
    }

    /**
     * Likes a danmu.
     * If the user already liked the danmu, this operation will cancel the like status.
     * It is mandatory that the user shall watch the video first before he/she can like a danmu of it.
     *
     * @param auth the current user's authentication information
     * @param id   the danmu's id
     * @return the like state of the user to this danmu after this operation
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
     *   <li>cannot find a danmu corresponding to the {@code id}</li>
     * </ul>
     * If any of the corner case happened, {@code false} shall be returned.
     */
    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (!validAuth(auth)) return false;
        if (!existDanmu(id)) return false;

        boolean like =false;
        String findLikes = "select count(*) from danmulikeby where danmu_id = ? and mid=?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(findLikes)) {
                stmt.setLong(1, id);
                stmt.setLong(2,auth.getMid());
                ResultSet resultSet = stmt.executeQuery();
                while (resultSet.next()) {
                    like=resultSet.getInt(1)==1;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        if(like){
            String deleteLikes = "delete from danmulikeby where danmu_id = ? and id=?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(deleteLikes)) {
                stmt.setLong(1, id);
                stmt.setLong(2,auth.getMid());
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }else{
            String deleteLikes = "insert into danmulikeby values (? ,?) ";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(deleteLikes)) {
                stmt.setLong(1, id);
                stmt.setLong(2,auth.getMid());
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        //            String deleteLikes = "delete from danmulikeby where danmu_id = ? and id=?";
        //            try (Connection conn = dataSource.getConnection();
        //                 PreparedStatement stmt = conn.prepareStatement(deleteLikes)) {
        //                stmt.setLong(1, id);
        //                stmt.setLong(2,auth.getMid());
        //                stmt.executeUpdate();
        //            } catch (SQLException e) {
        //                throw new RuntimeException(e);
        //            }
        //            String deleteLikes = "insert into danmulikeby values (? ,?) ";
        //            try (Connection conn = dataSource.getConnection();
        //                 PreparedStatement stmt = conn.prepareStatement(deleteLikes)) {
        //                stmt.setLong(1, id);
        //                stmt.setLong(2,auth.getMid());
        //                stmt.executeUpdate();
        //            } catch (SQLException e) {
        //                throw new RuntimeException(e);
        //            }
        return !like;

//        String updateVideo = "UPDATE video SET likes = likes + 1 WHERE bv = ?";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(updateVideo)) {
//            stmt.setString(1, bv);
//            stmt.executeUpdate();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        // update the like in the thumbs_up table
//        String insertLikes = "insert into thumbs_up (video_BV, user_mid) values (?,?)";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(insertLikes)) {
//
//            stmt.setString(1, bv);
//            stmt.setLong(2, auth.getMid());
//
//            int rowsAffected = stmt.executeUpdate();
//            return rowsAffected > 0;
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//
//        String sqlOfMaxID = " select max(danamu_id) as max from danmu";
//        long MaxMid = 0L;
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement statement = conn.prepareStatement(sqlOfMaxID);
//             ResultSet resultSet = statement.executeQuery()) {
//            while (resultSet.next()) {
//                MaxMid = resultSet.getLong("max");
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException();
//        }



//        int cnt = 0;
//        String searchDanmu = "select count(*) as cnt from danmulikeby where danmu_id = ? and mid = ? ";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(searchDanmu)) {
//            stmt.setLong(1, id);
//            stmt.setLong(2, auth.getMid());
//            ResultSet resultSet = stmt.executeQuery();
//            while (resultSet.next()) {
//                cnt = resultSet.getInt(1);
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//
//        if (cnt == 1) {
//            String deleteDanmu = "delete from danmulikeby where danmu_id = ? and mid = ? ";
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(deleteDanmu)) {
//                stmt.setLong(1, id);
//                stmt.setLong(2, auth.getMid());
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//            return false;
//        } else {
//            String insertVideo = "INSERT INTO danmulikeby values (?,?) ";
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(insertVideo)) {
//                stmt.setLong(1, id);
//                stmt.setLong(2, auth.getMid());
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//            return true;
//        }
    }

    public boolean validAuth(AuthInfo auth) {
        //valid of mid
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

    public boolean existMid(long mid) {
        String sqlOfMid = "select count(*) as count from users where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery(sqlOfMid);
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid != 1;
    }

    public boolean existQQ(String QQ) {
        String sqlOfMid = "select count(*) as count from users where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setString(1, QQ);
            ResultSet resultSet = stmt.executeQuery(sqlOfMid);
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid == 1;
    }

    public boolean existWechat(String Wechat) {
        String sqlOfMid = "select count(*) as count from users where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setString(1, Wechat);
            ResultSet resultSet = stmt.executeQuery(sqlOfMid);
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid == 1;
    }

    public boolean existBv(String bv) {
        String sqlOfBv = "select count(*) as cnt from video where bv = ?";
        int numberOfBv = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfBv)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfBv = resultSet.getInt("cnt");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfBv > 0;
    }

    public boolean existDanmu(long id) {
        String sqlOfBv = "select count(*) as count from danmu where id = ?";
        int numberOfBv = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfBv)) {
            stmt.setLong(1, id);

            ResultSet resultSet = stmt.executeQuery(sqlOfBv);
            if (resultSet.next()) {
                numberOfBv = resultSet.getInt("count");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfBv == 1;
    }

    public boolean bvPublicTime(String bv) {
        String sqlOfPublic = "select public_time::text as public_time_ from video where bv = ?";
        String current = "select current_timestamp::text as current_time_";
        String publicTime = "";
        String currentTime = "";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfPublic)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                publicTime = resultSet.getString("public_time_");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(current)) {
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                currentTime = resultSet.getString("current_time_");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return publicTime.compareTo(currentTime) < 0;
    }
}
