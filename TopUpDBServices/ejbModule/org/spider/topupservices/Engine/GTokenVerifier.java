package org.spider.topupservices.Engine;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken.Payload;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;

public class GTokenVerifier {
	private JsonFactory mJFactory = new GsonFactory();
	String CLIENT_ID = "263807486495-kjkqg79euuorrdavj6i218o0s5cl52bm.apps.googleusercontent.com";
//	GTokenVerifier(String CLIENT_ID){
//		this.CLIENT_ID = CLIENT_ID;
//	}

	public boolean verifyGToken(String idTokenString) {
		boolean valid = false;
		NetHttpTransport transport = new NetHttpTransport();
		GoogleIdTokenVerifier verifier = new GoogleIdTokenVerifier.Builder(transport, mJFactory)
				// Specify the CLIENT_ID of the app that accesses the backend:
				.setAudience(Collections.singletonList(CLIENT_ID))
				// Or, if multiple clients access the backend:
				// .setAudience(Arrays.asList(CLIENT_ID_1, CLIENT_ID_2, CLIENT_ID_3))
				.build();

		// (Receive idTokenString by HTTPS POST)

		GoogleIdToken idToken;
		try {
			idToken = verifier.verify(idTokenString);

			if (idToken != null) {
				Payload payload = idToken.getPayload();

				// Print user identifier
				String userId = payload.getSubject();
				System.out.println("User ID: " + userId);

				// Get profile information from payload
				String email = payload.getEmail();
				boolean emailVerified = Boolean.valueOf(payload.getEmailVerified());
				String name = (String) payload.get("name");
				String pictureUrl = (String) payload.get("picture");
				String locale = (String) payload.get("locale");
				String familyName = (String) payload.get("family_name");
				String givenName = (String) payload.get("given_name");
				
				valid = true;
				System.out.println("Token ID Valid.");
				System.out.println("Email : "+email + "\nemailVerified : "+emailVerified +"\nname : "+name +"\npictureUrl : "+pictureUrl +"\nlocale : "+locale +"\nfamilyName : "+familyName +"\ngivenName : "+givenName);
			} else {
				valid = false;
				System.out.println("Invalid ID token.");
			}
		} catch (GeneralSecurityException e) {
			// TODO Auto-generated catch block
			valid = false;
			e.printStackTrace();
		} catch (IOException e) {
			valid = false;
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return valid;
	}
}
