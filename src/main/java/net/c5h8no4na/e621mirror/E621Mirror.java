package net.c5h8no4na.e621mirror;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import net.c5h8no4na.common.assertion.Assert;
import net.c5h8no4na.e621.api.E621Client;
import net.c5h8no4na.e621.api.E621Response;
import net.c5h8no4na.e621.api.response.FullUserApi;
import net.c5h8no4na.e621.api.response.PostApi;
import net.c5h8no4na.e621.api.response.TagApi;
import net.c5h8no4na.e621.api.response.UserApi;
import net.c5h8no4na.entity.e621.DestroyedPost;
import net.c5h8no4na.entity.e621.Post;
import net.c5h8no4na.entity.e621.PostFile;
import net.c5h8no4na.entity.e621.Source;
import net.c5h8no4na.entity.e621.Tag;
import net.c5h8no4na.entity.e621.Tag_;
import net.c5h8no4na.entity.e621.User;
import net.c5h8no4na.entity.e621.enums.Extension;
import net.c5h8no4na.entity.e621.enums.Level;
import net.c5h8no4na.entity.e621.enums.Rating;
import net.c5h8no4na.entity.e621.enums.TagType;

public class E621Mirror {
	private EntityManager em;

	private E621Client client = new E621Client("earlopain/mirror");

	E621Mirror(EntityManager em) {
		this.em = em;
	}

	public Post findOrCreatePost(int id) throws InterruptedException, IOException {
		if (postIsDestroyed(id)) {
			return null;
		}
		Post p = em.find(Post.class, id);
		if (p == null) {
			return createPost(id);
		} else {
			return p;
		}
	}

	public Post findOrCreatePost(PostApi post) throws InterruptedException, IOException {
		if (postIsDestroyed(post.getId())) {
			return null;
		}
		Post p = em.find(Post.class, post.getId());
		if (p == null) {
			return createPost(post);
		} else {
			return p;
		}
	}

	public void createDestroyedPost(int id) {
		DestroyedPost dp = new DestroyedPost();
		dp.setId(id);
		em.persist(dp);
	}

	public User findOrCreateUser(int id) throws InterruptedException, IOException {
		User u = em.find(User.class, id);
		if (u == null) {
			return createUser(id);
		} else {
			return u;
		}
	}

	public User findOrCreateUser(UserApi user) throws InterruptedException, IOException {
		User u = em.find(User.class, user.getId());
		if (u == null) {
			return createUser(user);
		} else {
			return u;
		}
	}

	private boolean postIsDestroyed(Integer id) {
		return em.find(DestroyedPost.class, id) != null;
	}

	private Post createPost(int id) throws InterruptedException, IOException {
		E621Response<PostApi> response = client.getPost(id);
		if (response.isSuccess()) {
			PostApi post = response.unwrap();
			return createPost(post);
		} else {
			if (response.getResponseCode() == 404) {
				createDestroyedPost(id);
			}
			return null;
		}
	}

	private Post createPost(PostApi post) throws InterruptedException, IOException {
		Post dbPost = new Post();
		dbPost.setId(post.getId());
		dbPost.setCreatedAt(new Timestamp(post.getCreatedAt().getTime()));
		dbPost.setUpdatedAt(post.getUpdatedAt() == null ? null : new Timestamp(post.getUpdatedAt().getTime()));
		dbPost.setWidth(post.getFile().getWidth());
		dbPost.setHeight(post.getFile().getHeight());
		dbPost.setExtension(Extension.from(post.getFile().getExt()).get());
		dbPost.setSize(post.getFile().getSize());
		dbPost.setMd5(post.getFile().getMd5());
		dbPost.setScoreUp(post.getScore().getUp());
		dbPost.setScoreDown(post.getScore().getDown());
		dbPost.setScoreTotal(post.getScore().getTotal());
		dbPost.setTags(findOrCreateTags(post.getTags().getAll()));
		dbPost.setRating(Rating.from(post.getRating()).get());
		dbPost.setFavCount(post.getFavCount());
		dbPost.setDescription(post.getDescription());

		em.persist(dbPost);
		if (post.getApproverId().isPresent()) {
			dbPost.setApprover(findOrCreateUser(post.getApproverId().get()));
		}
		dbPost.setUploader(findOrCreateUser(post.getUploaderId()));
		Assert.notNull(dbPost.getUploader());
		dbPost.setDuration(post.getDuration().orElse(null));

		Optional<byte[]> fileToInsert = client.getFile(post.getFile().getMd5(), post.getFile().getExt());
		if (fileToInsert.isPresent()) {
			PostFile pf = new PostFile();
			pf.setPost(dbPost);
			pf.setFile(fileToInsert.get());
			em.persist(pf);
			dbPost.setPostFile(pf);
		}

		// Children
		List<Post> children = new ArrayList<>();

		for (Integer childId : post.getRelationships().getChildren()) {
			children.add(findOrCreatePost(childId));
		}

		dbPost.setChildren(children);
		for (String source : post.getSources()) {
			Source s = new Source();
			s.setPost(dbPost);
			s.setSource(source);
			em.persist(s);
		}
		return dbPost;
	}

	private User createUser(int id) throws InterruptedException, IOException {
		E621Response<FullUserApi> response = client.getUserById(id);
		if (response.isSuccess()) {
			FullUserApi user = response.unwrap();
			return createUser(user);
		} else {
			return null;
		}
	}

	private User createUser(UserApi user) throws InterruptedException, IOException {
		User dbUser = new User();
		dbUser.setId(user.getId());
		dbUser.setCreatedAt(new Timestamp(user.getCreatedAt().getTime()));
		dbUser.setName(user.getName());
		dbUser.setLevel(Level.from(user.getLevel()).get());
		dbUser.setIsBanned(user.getIsBanned());
		em.persist(dbUser);
		if (user.getAvatarId().isPresent()) {
			dbUser.setAvatar(findOrCreatePost(user.getAvatarId().get()));
		}
		return dbUser;
	}

	private List<Tag> findOrCreateTags(List<String> tags) throws InterruptedException, IOException {
		List<Tag> alreadyPersistedTags = getTagsByName(tags);

		for (Tag tag : alreadyPersistedTags) {
			if (tags.contains(tag.getText())) {
				tags.remove(tag.getText());
			}
		}

		// Seperate the list into many smaller lists
		int chunkSize = 50;
		List<List<String>> chunks = new ArrayList<>();
		for (int i = 0; i < tags.size(); i += chunkSize) {
			chunks.add(tags.subList(i, Math.min(i + chunkSize, tags.size())));
		}

		for (List<String> list : chunks) {
			List<TagApi> response = client.getTagsByName(list).unwrap();
			for (TagApi tag : response) {
				Tag dbTag = new Tag();
				dbTag.setId(tag.getId());
				dbTag.setTagType(TagType.from(tag.getCategory()).get());
				dbTag.setText(tag.getName());
				em.persist(dbTag);
				alreadyPersistedTags.add(dbTag);
			}
		}

		return alreadyPersistedTags;
	}

	private List<Tag> getTagsByName(List<String> tags) {
		CriteriaBuilder cb = em.getCriteriaBuilder();
		CriteriaQuery<Tag> cq = cb.createQuery(Tag.class);
		Root<Tag> root = cq.from(Tag.class);
		cq.where(root.get(Tag_.text).in(tags));
		TypedQuery<Tag> q = em.createQuery(cq);
		return q.getResultList();
	}
}
