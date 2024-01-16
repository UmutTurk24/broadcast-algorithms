#![allow(non_camel_case_types)]
use libp2p_server::{P2PServer, ClientBehaviour};

use ark_bls12_381::Bls12_381;
use ark_poly_commit::{Polynomial, marlin_pc::MarlinKZG10, LabeledPolynomial, PolynomialCommitment, QuerySet, Evaluations, challenge::ChallengeGenerator};
use ark_crypto_primitives::sponge::poseidon::{PoseidonSponge, PoseidonConfig};
use ark_crypto_primitives::sponge::CryptographicSponge;
use ark_ec::pairing::Pairing;
use ark_ff::UniformRand;
use ark_std::test_rng;
use ark_poly::{univariate::DensePolynomial, DenseUVPolynomial};
use rand_chacha::ChaCha20Rng;
use ark_ff::PrimeField;
use ark_bls12_381::Fr;

/* 
Reliable Broadcast
    - Sender sends a message to all peers it is connected to
    - Each peer sends the message, if it has not already seen it, to all peers it is connected to
    - Once a peer receives 1/3 of the messages, it sends an acknowledgement to all peers it is connected to
    - Once a peer receives 2/3 of the acknowledgements, it sends a commit message to all peers it is connected to
    - Once a peer receives 2/3 of the commit messages, it commits the message
*/
type UniPoly_381 = DensePolynomial<<Bls12_381 as Pairing>::ScalarField>;
type Sponge_Bls12_381 = PoseidonSponge<<Bls12_381 as Pairing>::ScalarField>;
type PCS = MarlinKZG10<Bls12_381, UniPoly_381, Sponge_Bls12_381>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 

    // Run the server
    P2PServer::initialize_server(Some("./src/bootstrap-nodes.txt".to_string()), ClientBehaviour::ReliableBroadcast).await?;

    
    // let mut challenge_generator = ChallengeGenerator::<<Bls12_381 as Pairing>::ScalarField,PoseidonSponge<<Bls12_381 as Pairing>::ScalarField>>::Univariate((), ())
    // let challenge_generator: ChallengeGenerator<<Bls12_377 as Pairing>::ScalarField, Sponge_Bls12_377> = ChallengeGenerator::new_univariate(&mut test_sponge);

    // let mut challenge = challenge_generator.get_challenge();


    // let mut evals = secret_poly.evaluate(&[Fr::from(1), Fr::from(2)]);


    Ok(())
}
