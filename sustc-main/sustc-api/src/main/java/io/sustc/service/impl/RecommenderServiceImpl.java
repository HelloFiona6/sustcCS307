package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
    @Autowired
    private DataSource dataSource;

    @Override
    public List<String> recommendNextVideo(String bv) {
//        String sqlOfBv = "select count(*) as count from video where bv=?";
//        int numberOfBv = 0;
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sqlOfBv)) {
//            stmt.setString(1, bv);
//            ResultSet resultSet = stmt.executeQuery();
//            if (resultSet.next()) {
//                numberOfBv = resultSet.getInt("count");
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        if (numberOfBv == 0) return null;
//
//        //BvViewer
//        String sqlOfBvViewer = "select user_mid as user_mid from video where bv=?";
//        List<String> listOfViewer = new ArrayList<>();
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sqlOfBvViewer)) {
//            stmt.setString(1, bv);
//            ResultSet resultSet = stmt.executeQuery();
//            while (resultSet.next()) {
//                listOfViewer.add(resultSet.getString("user_mid"));
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//
//        StringBuilder viewer = new StringBuilder();
//        for (int i = 0; i < listOfViewer.size(); i++) {
//            if (i != listOfViewer.size() - 1) viewer.append(listOfViewer.get(i)).append(",");
//            else viewer.append(listOfViewer.get(i));
//        }
//        viewer = new StringBuilder("(" + viewer + ")");
//
//
//        //BvList
//        String sqlOfBvList = "select user_mid from video where bv <> ?";
//        List<String> listOfBv = new ArrayList<>();
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sqlOfBvList)) {
//            stmt.setString(1, bv);
//            ResultSet resultSet = stmt.executeQuery();
//            while (resultSet.next()) {
//                listOfBv.add(resultSet.getString("user_mid"));
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//
//
//        List<node> temp = new ArrayList<>();
//        String sqlOfViewWithNumber = "select count(*) as count from view where video_BV = ? and user_mid in ?";
//        for (int i = 0; i < listOfBv.size(); i++) {
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(sqlOfViewWithNumber)) {
//                stmt.setString(1, listOfBv.get(i));
//                stmt.setString(2, String.valueOf(viewer));
//                ResultSet resultSet = stmt.executeQuery();
//                while (resultSet.next()) {
//                    temp.add(new node(listOfBv.get(i), resultSet.getInt("count")));
//                }
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        Collections.sort(temp);
//        List<String> result = new ArrayList<>();
//        for (int i = 0; i < Math.min(5, temp.size()); i++) {
//            result.add(temp.get(i).bv);
//        }

        ArrayList<String> arrayList = new ArrayList<>();
        String sql = "select * from recommend_next_video(?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Array array = resultSet.getArray(1);
                String[] values = (String[]) array.getArray();
                arrayList = new ArrayList<>(Arrays.asList(values));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if (pageNum <= 0 || pageSize <= 0) return null;
//        //BvList
//        String sqlOfBvList = "select user_mid from video";
//        List<String> listOfBv = new ArrayList<>();
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sqlOfBvList)) {
//            ResultSet resultSet = stmt.executeQuery(sqlOfBvList);
//            while (resultSet.next()) {
//                listOfBv.add(resultSet.getString("user_mid"));
//            }
//            stmt.executeUpdate();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//
//        List<Node> temp = new ArrayList<>();
//        for (int i = 0; i < listOfBv.size(); i++) {
//            double sum = 0;
//            //like
//            String sqlOfLike = """
//                    select count(*) as count from thumbs_up where video_bv = ?
//                    """;
//            int countOfLike = 0;
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(sqlOfLike)) {
//                stmt.setString(1, listOfBv.get(i));
//                ResultSet resultSet = stmt.executeQuery();
//                while (resultSet.next()) {
//                    countOfLike = resultSet.getInt("count");
//                }
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//
//            //coin
//            String sqlOfCoin = """
//                    select count(*) as count from coin where video_bv = ?
//                    """;
//            int countOfCoin = 0;
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(sqlOfCoin)) {
//                stmt.setString(1, listOfBv.get(i));
//                ResultSet resultSet = stmt.executeQuery();
//                while (resultSet.next()) {
//                    countOfCoin = resultSet.getInt("count");
//                }
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//
//
//            //fav
//            String sqlOfFav = """
//                    select count(*) as count from favorite where video_bv = ?
//                    """;
//            int countOfFav = 0;
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(sqlOfFav)) {
//                stmt.setString(1, listOfBv.get(i));
//                ResultSet resultSet = stmt.executeQuery();
//                while (resultSet.next()) {
//                    countOfFav = resultSet.getInt("count");
//                }
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//
//            //danmu
//            String sqlOfDanmu = """
//                    select count(*) as count from favorite where bv = ?
//                    """;
//            int countOfDanmu = 0;
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(sqlOfDanmu)) {
//                stmt.setString(1, listOfBv.get(i));
//                ResultSet resultSet = stmt.executeQuery();
//                while (resultSet.next()) {
//                    countOfDanmu = resultSet.getInt("count");
//                }
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//
//            //finish
//            String sqlOfFinish = """
//                    select count(*) count from view left join video on view.video_BV = video.BV where last_watch_time_duration=duration and bv=?
//                    """;
//            int countOfFinish = 0;
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(sqlOfFinish)) {
//                stmt.setString(1, listOfBv.get(i));
//                ResultSet resultSet = stmt.executeQuery();
//                while (resultSet.next()) {
//                    countOfFinish = resultSet.getInt("count");
//                }
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//
//            //total
//            String sqlOfView = """
//                    select count(*) as count from view where video_bv = ?
//                    """;
//            int countOfView = 0;
//            try (Connection conn = dataSource.getConnection();
//                 PreparedStatement stmt = conn.prepareStatement(sqlOfView)) {
//                stmt.setString(1, listOfBv.get(i));
//                ResultSet resultSet = stmt.executeQuery();
//                while (resultSet.next()) {
//                    countOfView = resultSet.getInt("count");
//                }
//                stmt.executeUpdate();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//            sum += countOfLike * 1.0 / countOfView;
//            sum += countOfCoin * 1.0 / countOfView;
//            sum += countOfFav * 1.0 / countOfView;
//            sum += countOfDanmu * 1.0 / countOfView;
//            sum += countOfFinish * 1.0 / countOfView;
//            temp.add(new Node(listOfBv.get(i), sum));
//        }
//
//        Collections.sort(temp);
//        List<String> result = new ArrayList<>();
//        for (int i = 0; i < Math.min(temp.size(), pageNum * pageSize); i++) {
//            result.add(temp.get(i).bv);
//        }
        ArrayList<String> arrayList = new ArrayList<>();
        String sql = "select * from general_recommendations( ?,? )";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, pageSize);
            stmt.setLong(2, pageNum);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Array array = resultSet.getArray(1);
                String[] values = (String[]) array.getArray();
                arrayList = new ArrayList<>(Arrays.asList(values));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        if (!validAuth(auth)) return null;
        if (pageSize <= 0 || pageNum <= 0) return null;
        List<String> arrayList = new ArrayList<>();
        String sql = "select * from recommend_videos_for_user( ? , ? , ? )";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, pageSize);
            stmt.setLong(3, pageNum);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Array array = resultSet.getArray(1);
                String[] values = (String[]) array.getArray();
                arrayList = new ArrayList<>(Arrays.asList(values));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        if (!validAuth(auth)) return null;
        if (pageSize <= 0 || pageNum <= 0) return null;
        ArrayList<Long> arrayList = new ArrayList<>();
        String sql = "select * from Recommend_Friends( ? , ? , ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            stmt.setLong(2, pageSize);
            stmt.setLong(3, pageNum);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next()) {
                Array array = resultSet.getArray(1);
                Long[] values = (Long[]) array.getArray();
                arrayList = new ArrayList<>(Arrays.asList(values));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return arrayList;
    }

    private static class node implements Comparable<node> {
        String bv;
        int val;
        double sum;

        public node(String bv, int val) {
            this.bv = bv;
            this.val = val;
        }

        @Override
        public int compareTo(node other) {
            return other.val - this.val;
        }
    }

    private static class Node implements Comparable<Node> {
        String bv;
        double sum;

        public Node(String bv, double sum) {
            this.bv = bv;
            this.sum = sum;
        }

        @Override
        public int compareTo(Node other) {
            return Double.compare(other.sum, this.sum);
        }
    }

    private boolean validAuth(AuthInfo auth) {
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
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid != 1;
    }

    public boolean existQQ(String QQ) {
        String sqlOfMid = "select count(*) as count from users where QQ= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setString(1, QQ);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid == 1;
    }

    public boolean existWechat(String Wechat) {
        String sqlOfMid = "select count(*) as count from users where wechat= ?";
        int numberOfMid = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sqlOfMid)) {
            stmt.setString(1, Wechat);
            ResultSet resultSet = stmt.executeQuery();
            if (resultSet.next()) {
                numberOfMid = resultSet.getInt("count");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return numberOfMid == 1;
    }
}