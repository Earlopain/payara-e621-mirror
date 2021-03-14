package net.c5h8no4na.e621mirror;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.ejb.Schedule;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import net.c5h8no4na.e621.api.E621Client;
import net.c5h8no4na.e621.api.E621Response;
import net.c5h8no4na.e621.api.response.PostApi;
import net.c5h8no4na.entity.e621.Post;
import net.c5h8no4na.entity.e621.Post_;

@Singleton
@Startup
public class GetNewPostsTimer {
	private static final Logger LOG = Logger.getLogger(GetNewPostsTimer.class.getCanonicalName());

	private E621Mirror mirror;

	@PersistenceContext(unitName = "e621")
	private EntityManager em;

	private E621Client client = new E621Client("earlopain/mirror");

	private int dbHighestPost;

	@PostConstruct
	public void init() {
		mirror = new E621Mirror(em);
		dbHighestPost = getLatestDatabase();
	}

	@Schedule(minute = "*/2", hour = "*", persistent = false)
	public void getNewPosts() throws InterruptedException, IOException {
		int targetId = getLatestApi();
		int currentId = dbHighestPost;
		if (targetId == currentId) {
			return;
		}
		LOG.info(String.format("Getting new posts %s-%s", currentId + 1, targetId));

		while (currentId != targetId) {
			List<PostApi> posts = client.getPostsByTagsAfterId(List.of("status:any"), currentId, 320).unwrap();

			for (PostApi post : posts) {
				mirror.findOrCreatePost(post);
				// send to db an clear refenreces in the em, Posts can have large blobs
				// associated with them, no need to fill the heap with that
				em.flush();
				em.clear();
			}
			int newCurrentId = posts.stream().mapToInt(post -> post.getId()).max().getAsInt();
			if (newCurrentId == currentId) {
				LOG.warning(String.format("Loop while getting %s, going from %s-%s", currentId, dbHighestPost + 1, targetId));
				break;
			}
			currentId = newCurrentId;
		}
		dbHighestPost = targetId;
		LOG.info("Finished getting new posts");
	}

	private int getLatestApi() throws InterruptedException, IOException {
		E621Response<List<PostApi>> a = client.getPostsByTags(List.of(), 1);
		return a.unwrap().get(0).getId();
	}

	private int getLatestDatabase() {
		CriteriaBuilder cb = em.getCriteriaBuilder();
		CriteriaQuery<Integer> cq = cb.createQuery(Integer.class);
		Root<Post> root = cq.from(Post.class);
		cq.select(root.get(Post_.id));
		cq.orderBy(cb.desc(root.get(Post_.id)));

		TypedQuery<Integer> q = em.createQuery(cq);
		q.setMaxResults(1);
		return q.getSingleResult();
	}
}
