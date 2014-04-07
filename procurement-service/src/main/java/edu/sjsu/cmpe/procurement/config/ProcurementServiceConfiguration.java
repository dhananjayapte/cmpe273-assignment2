package edu.sjsu.cmpe.procurement.config;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.yammer.dropwizard.client.JerseyClientConfiguration;
import com.yammer.dropwizard.config.Configuration;

public class ProcurementServiceConfiguration extends Configuration {
    @NotEmpty
    @JsonProperty
    private String stompQueueName;

    @NotEmpty
    @JsonProperty
    private String stompTopicPrefix;

    @Valid
    @NotNull
    @JsonProperty
    private JerseyClientConfiguration httpClient = new JerseyClientConfiguration();
    
    private String apolloUser;
    
    private String apolloPassword;
    
    private String apolloHost;
    
    private String apolloPort;
    
    private String stompTopicAll;
    
    private String stompTopicComputer;

    /**
     * 
     * @return
     */
    public JerseyClientConfiguration getJerseyClientConfiguration() {
	return httpClient;
    }

    /**
     * @return the stompQueueName
     */
    public String getStompQueueName() {
	return stompQueueName;
    }

    /**
     * @param stompQueueName
     *            the stompQueueName to set
     */
    public void setStompQueueName(String stompQueueName) {
	this.stompQueueName = stompQueueName;
    }

    public String getStompTopicPrefix() {
	return stompTopicPrefix;
    }

    public void setStompTopicPrefix(String stompTopicPrefix) {
	this.stompTopicPrefix = stompTopicPrefix;
    }

	/**
	 * @return the apolloUser
	 */
	public String getApolloUser() {
		return apolloUser;
	}

	/**
	 * @param apolloUser the apolloUser to set
	 */
	public void setApolloUser(String apolloUser) {
		this.apolloUser = apolloUser;
	}

	/**
	 * @return the apolloPassword
	 */
	public String getApolloPassword() {
		return apolloPassword;
	}

	/**
	 * @param apolloPassword the apolloPassword to set
	 */
	public void setApolloPassword(String apolloPassword) {
		this.apolloPassword = apolloPassword;
	}

	/**
	 * @return the apolloHost
	 */
	public String getApolloHost() {
		return apolloHost;
	}

	/**
	 * @param apolloHost the apolloHost to set
	 */
	public void setApolloHost(String apolloHost) {
		this.apolloHost = apolloHost;
	}

	/**
	 * @return the apolloPort
	 */
	public String getApolloPort() {
		return apolloPort;
	}

	/**
	 * @param apolloPort the apolloPort to set
	 */
	public void setApolloPort(String apolloPort) {
		this.apolloPort = apolloPort;
	}

	/**
	 * @return the stompTopicAll
	 */
	public String getStompTopicAll() {
		return stompTopicAll;
	}

	/**
	 * @param stompTopicAll the stompTopicAll to set
	 */
	public void setStompTopicAll(String stompTopicAll) {
		this.stompTopicAll = stompTopicAll;
	}

	/**
	 * @return the stompTopicComputer
	 */
	public String getStompTopicComputer() {
		return stompTopicComputer;
	}

	/**
	 * @param stompTopicComputer the stompTopicComputer to set
	 */
	public void setStompTopicComputer(String stompTopicComputer) {
		this.stompTopicComputer = stompTopicComputer;
	}

}
