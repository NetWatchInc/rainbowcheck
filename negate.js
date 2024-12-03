// Utility script to create a negation label for a user account DID

import { LabelerServer } from 'skyware';

const credentials = {
	did: 'Labeler DID',
	signingKey: 'Labeler Signing Key',
};

async function createNegationLabel() {
	const labelerServer = new LabelerServer(credentials);

	await labelerServer.createLabel({
		uri: 'User Account DID',
		val: '', // identifier of the label to negate
		neg: true,
		src: credentials.did,
	});
}

createNegationLabel().catch(console.error);
