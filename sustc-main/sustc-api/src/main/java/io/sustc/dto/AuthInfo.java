package io.sustc.dto;

import java.io.Serializable;

import lombok.*;

/**
 * The authorization information class
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class AuthInfo implements Serializable {

    /**
     * The user's mid.
     */
    private long mid;

    /**
     * The password used when login by mid.
     */
    private String password;

    /**
     * OIDC login by QQ, does not require a password.
     */
    private String qq;

    /**
     * OIDC login by WeChat, does not require a password.
     */
    private String wechat;

}
