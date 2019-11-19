package com.pyspark;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.databind.ObjectMapper;

public class mail {
	static String username = "vikram@domain.com";
	static String password = "xxxxxxxxx";
	static String mailFrom = "vikram@domain.com";
	static String mailTo = "smahadevan@domain.com";

	public static void main(String[] args) throws IOException {
		// YAML Properties:

		// for loop
		int w;

//		NodeList Prop = docEle.getElementsByTagName("Properties");
//		Element Properties = (Element) Prop.item(0);
//
//		String SMTP_AUTH = Properties.getAttribute("smtpauth");
//		String SMTP_TLS_ENABLE = Properties.getAttribute("smtpenable");
//		String SMTP_HOST = Properties.getAttribute("smtphost");
//		String SMTP_PORT = Properties.getAttribute("smtpport");
//		username = Properties.getAttribute("username");
//		password = Properties.getAttribute("mailpassword");
//		mailFrom = Properties.getAttribute("mailFrom");
//		mailTo = Properties.getAttribute("mailTo");
		Properties props = new Properties();
		 String smtpauth= "true";
		 String smtpenable= "true";
		 String smtphost= "smtp.office365.com";
	     String smtpport= "587";

		props.put("mail.smtp.auth", smtpauth);
		props.put("mail.smtp.starttls.enable", smtpenable);
		props.put("mail.smtp.host", smtphost);
		props.put("mail.smtp.port", smtpport);

		Session session = Session.getInstance(props, new javax.mail.Authenticator() {

			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(username, password);
			}

		});

		try {
			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(mailFrom));
			message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(mailTo));
			// message.setRecipients(Message.RecipientType.TO,
			// InternetAddress.parse("vikram@domain"));
			

			message.setSubject("Critical Alert");
			message.setContent("<h:body style=background-color:white;font-family:verdana;color:#0066CC;>"
					+ "Testing Alert : " + "test email from vikram" + " !<br/><br/>", "text/html; charset=utf-8");
			Transport.send(message);

			System.out.println("Email was sent");

		} catch (MessagingException e) {
			System.out.println("Could not parse the message. ");
		}

	}

}
