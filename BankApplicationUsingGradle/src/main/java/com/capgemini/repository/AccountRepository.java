package com.capgemini.repository;

import com.capgemini.model.Account;

public interface AccountRepository {
	
	boolean createAccount(Account account);
		
	Account searchAccount(int accountNumber);

}
