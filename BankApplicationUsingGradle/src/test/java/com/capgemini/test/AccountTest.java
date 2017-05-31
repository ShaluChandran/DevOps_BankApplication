package com.capgemini.test;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.capgemini.exceptions.InsufficientInitialBalanceException;
import com.capgemini.model.Account;
import com.capgemini.repository.AccountRepository;
import com.capgemini.service.AccountService;
import com.capgemini.service.AccountServiceImpl;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

public class AccountTest {
	
	@Mock
	AccountRepository accountRepository;
	
	AccountService accountService;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);//dummy object accountRepository object is created
		accountService = new AccountServiceImpl(accountRepository);
	}
	
	// use story for createAccount
	//1 when the amount is less than 500 then it should throw exception
	//2 when the valid info is passed account object should be created successfully
	
	@Test(expected = com.capgemini.exceptions.InsufficientInitialBalanceException.class)
	public void whenTheAmountIsLessThan500ItShouldThrowException() throws InsufficientInitialBalanceException
	{
		accountService.createAccount(101,200);
		
	}
	
	@Test
	public void whenTheValidInfoIsPassedAccountObjectShouldBeCreatedSuccessfully() throws InsufficientInitialBalanceException
	{
		Account account = new Account();
		
		account.setAccountNumber(101);
		account.setAmount(5000);
		
		when(accountRepository.createAccount(account)).thenReturn(true);
		
		assertEquals(account.getAmount(),accountService.createAccount(101, 5000).getAmount());
	}
	
// when the valid info info is passed , amount should be deposited
	
	//when the balance is insufficient it should throw exception

	

}
