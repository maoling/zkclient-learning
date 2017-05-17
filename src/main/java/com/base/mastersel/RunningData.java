package com.base.mastersel;

import java.io.Serializable;

/**
 * 用于描述一个服务器节点的基本信息
 * @author maoling
 *
 */
public class RunningData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4260577459043203630L;

	private Long cid;
	private String name;
	public Long getCid() {
		return cid;
	}
	public void setCid(Long cid) {
		this.cid = cid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
}
