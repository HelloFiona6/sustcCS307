package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;

    @Override
    public long register(RegisterUserReq req) {
        if (req.getPassword() == null || req.getName() == null || req.getSex() == null) {
            return -1L;
        }
        int a = req.getBirthday().indexOf("月");
        int b = req.getBirthday().indexOf("日");
        if (a == -1 || b == -1 || a >= b) return -1;
        String month = req.getBirthday().substring(0, a);
        String day = req.getBirthday().substring(a + 1, b);
        int Month = Integer.parseInt(month);
        int Day = Integer.parseInt(day);
        if (!(Month >= 1 && Month <= 12)) return -1;
        int[][] arr = {
                {1, 31},
                {2, 29},
                {3, 31},
                {4, 30},
                {5, 31},
                {6, 30},
                {7, 31},
                {8, 31},
                {9, 30},
                {10, 31},
                {11, 30},
                {12, 31},
        };

        if (Day > arr[a - 1][1]) {
            return -1;
        }

        if (req.getQq() != null) {
            String sqlOfQQ = "select count(*) as count from users where QQ= " + req.getQq() + ";";
            int numberOfQQ = 0;
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement statement = conn.prepareStatement(sqlOfQQ);
                 ResultSet resultSet = statement.executeQuery(sqlOfQQ)) {

                while (resultSet.next()) {
                    numberOfQQ = resultSet.getInt("count");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (numberOfQQ != 0) return -1;
        }

        if (req.getWechat() != null) {
            String sqlOfWechat = " select count(*) as count from users where wechet= " + req.getWechat() + ";";
            int numberOfWechat = 0;
            try (Connection conn = dataSource.getConnection();
                 Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(sqlOfWechat)) {
                while (resultSet.next()) {
                    numberOfWechat = resultSet.getInt("count");
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (numberOfWechat != 0) return -1;
        }

        String sqlOfMaxMid = " select max(mid) as max from users";
        long MaxMid = 0L;
        try (Connection conn = dataSource.getConnection();
             Statement statement = conn.createStatement();
             ResultSet resultSet = statement.executeQuery(sqlOfMaxMid)) {
            while (resultSet.next()) {
                MaxMid = resultSet.getInt("max");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return MaxMid + 1;
    }


    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        if (!existMid(mid)) return false;

        // auth is invalid
        if (!validAuth(auth)) return false;

        String sqlOfIdentity = "select identity as identity from users where mid = ? ";
        String identityOfAuth = "";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfIdentity)) {
            stmt.setLong(1, auth.getMid());
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                identityOfAuth = resultSet.getString("identity");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String identity = "";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfIdentity)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                identity = resultSet.getString("identity");
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        if (identityOfAuth.equals("user") && auth.getMid() != mid) {
            return false;
        }
        if (identityOfAuth.equals("superuser") && (identity.equals("superUser") && mid != auth.getMid())) {
            return false;
        }

        String sqlOfDeleteCoin = "delete from coin where user_mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteCoin)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteDanmu = "delete from danmu where user_mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteDanmu)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteDanmulikeby = "delete from danmulikeby where mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteDanmulikeby)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteFavorite = "delete from favorite where user_mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteFavorite)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteFollow = "delete from coin where follower_mid = ? or following_mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteFollow)) {
            stmt.setLong(1, mid);
            stmt.setLong(2, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteThumbs_up = "delete from thumbs_up where user_mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteThumbs_up)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteUser = "delete from user where mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteUser)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteVideo = "delete from video where owner_mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteVideo)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String sqlOfDeleteView = "delete from view where user_mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfDeleteView)) {
            stmt.setLong(1, mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        // auth is invalid
        if (!validAuth(auth)) return false;
        if (auth.getMid() == followeeMid) return false;

        // followeeMid valid
        String sqlOfNumberOfFolloweeMidMid = "select count(*) as count from users where mid = ? ";
        int numberOfFolloweeMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfNumberOfFolloweeMidMid)) {
            stmt.setLong(1, followeeMid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfFolloweeMid = resultSet.getInt("count");
            }
        } catch (SQLException e) {
            throw new RuntimeException();
        }
        if (numberOfFolloweeMid != 1) return false;

        String sqlOfNumberOfMid = "select count(*) as count from follow where follower_mid = ? and following_mid = ?";
        int numberOfBoth = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfNumberOfMid)) {
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, followeeMid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfBoth = resultSet.getInt("count");
            }
        } catch (SQLException e) {
            throw new RuntimeException();
        }

        if (numberOfBoth == 1) {
            String sqlOfDelete = "delete from follow where follower_mid = ? and following_mid = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfDelete)) {
                stmt.setLong(1, auth.getMid());
                stmt.setLong(2, followeeMid);
                stmt.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException();
            }
            return false;
        } else {
            String sqlOfInsert = "insert into follow values (? , ?)";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlOfInsert)) {
                stmt.setLong(1, auth.getMid());
                stmt.setLong(2, followeeMid);
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return true;
        }
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        //mid
        String sqlOfNumberOfMid = "select count(*) as count from UserRecord where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfNumberOfMid)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (numberOfMid != 1) return null;

        //coin
        String sqlOfNumberOfCoin = """
                select count(owner_mid) from (
                select *
                from coin
                         left join video on coin.video_bv = video.bv)a;""";

        int numberOfCoin = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfNumberOfCoin)) {
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfCoin = resultSet.getInt("count");
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //following array
        String sqlOfFollowing = """
                select following_mid count from follow where follower_mid='?'
                """;
        ArrayList<Long> arrayListOfFollowing = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfFollowing)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfFollowing.add(resultSet.getLong("count"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long[] arrayOfFollowing = new long[arrayListOfFollowing.size()];
        for (int i = 0; i < arrayListOfFollowing.size(); i++) {
            arrayOfFollowing[i] = arrayListOfFollowing.get(i);
        }
        //follower
        String sqlOfFollower = """
                select follower_mid as count from follow where following_mid='?'
                """;
        ArrayList<Long> arrayListOfFollower = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfFollower)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfFollower.add(resultSet.getLong("count"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long[] arrayOfFollower = new long[arrayListOfFollower.size()];
        for (int i = 0; i < arrayOfFollower.length; i++) {
            arrayOfFollowing[i] = arrayListOfFollower.get(i);
        }

        //watch
        String sqlOfWatch = """
                select video_bv as bv from view where user_mid = ?
                """;
        ArrayList<String> arrayListOfWatch = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfWatch)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfWatch.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfWatch = arrayListOfWatch.toArray(new String[0]);


        //like
        String sqlOfLike = """
                select video_bv as bv from thumbs_up where user_mid = ?
                """;
        ArrayList<String> arrayListOfLike = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfLike)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfLike.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfLike = arrayListOfLike.toArray(new String[0]);

        //collect
        String sqlOfCollect = """
                select video_bv as bv from favorite where user_mid='?'
                """;
        ArrayList<String> arrayListOfCollect = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfCollect)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfCollect.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfCollect = arrayListOfCollect.toArray(new String[0]);

        //post
        String sqlOfPost = """
                select video_bv as bv from video where user_mid='?'            
                """;
        ArrayList<String> arrayListOfPost = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfPost)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                arrayListOfPost.add(resultSet.getString("bv"));
            }
            resultSet.close();
            stmt.close();
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        String[] arrayOfPost = arrayListOfPost.toArray(new String[0]);


        return new UserInfoResp(mid, numberOfCoin, arrayOfFollowing, arrayOfFollower, arrayOfWatch, arrayOfLike, arrayOfCollect, arrayOfPost);
    }


    public boolean existMid(long mid) {
        String sqlOfMid = "select count(*) as count from users where mid= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setLong(1, mid);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
//            resultSet.close();
//            stmt.close();
//            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid == 1;
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

    public boolean validAuth(AuthInfo auth) {
//        if ((auth.getQq() == null || auth.getQq().isEmpty()) && (auth.getWechat() == null || auth.getWechat().isEmpty()) && (auth.getPassword() == null || auth.getPassword().isEmpty()))
//            return false;
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
}